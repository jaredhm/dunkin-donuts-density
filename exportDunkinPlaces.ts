import {
  AddressType,
  Client,
  LatLngBounds,
  Place,
  PlaceType1,
} from "@googlemaps/google-maps-services-js";
import assert from "assert";
import axios from "axios";
import axiosRetry from "axios-retry";
import { config } from "dotenv";
import path from "path";
import pino from "pino";
import fs, { promises as fsp } from "fs";
import { parse } from "csv-parse";
import { AnyIterable, consume, pipeline, reduce, tap } from "streaming-iterables";

type ZipsFileRow = {
  zip: string | undefined;
  type: string | undefined;
  decommissioned: string | undefined;
  primary_city: string | undefined;
  acceptable_cities: string | undefined;
  unacceptable_cities: string | undefined;
  state: string | undefined;
  county: string | undefined;
  timezone: string | undefined;
  area_codes: string | undefined;
  world_region: string | undefined;
  country: string | undefined;
  latitude: string | undefined;
  longitude: string | undefined;
  irs_estimated_population: string | undefined;
};

const getEnv = () => {
  config();
  const apiKey = process.env.GOOGLE_MAPS_API_KEY;
  assert(typeof apiKey === "string");
  return {
    GOOGLE_MAPS_API_KEY: apiKey,
    LOG_LEVEL: process.env.LOG_LEVEL,
  };
};

axiosRetry(axios, {
  retries: 3,
  retryDelay: axiosRetry.exponentialDelay,
  retryCondition: (err) => (err.response?.status ?? 500) >= 400,
});
const client = new Client({ axiosInstance: axios });
const env = getEnv();
const logger = pino({
  level: env.LOG_LEVEL ?? "info",
});

const degreesToRadians = (deg: number) => Math.PI * (deg / 180);

// https://en.wikipedia.org/wiki/Haversine_formula#Formulation
const haversine = (bounds: LatLngBounds): number => {
  const r = 6371000; // meters
  const phi1 = degreesToRadians(bounds.northeast.lat);
  const phi2 = degreesToRadians(bounds.southwest.lat);
  const lambda1 = degreesToRadians(bounds.northeast.lng);
  const lambda2 = degreesToRadians(bounds.southwest.lng);

  return (
    2 *
    r *
    Math.asin(
      Math.sqrt(
        0.5 *
          (1 -
            Math.cos(phi2 - phi1) +
            Math.cos(phi1) * Math.cos(phi2) * (1 - Math.cos(lambda2 - lambda1)))
      )
    )
  );
};

const getPlacesFromZips = async function* (
  rows: AnyIterable<ZipsFileRow>
): AsyncIterable<Place> {
  let rowNum = 1;
  for await (const row of rows) {
    const { zip } = row;
    if (!zip) {
      logger.error(`Row ${rowNum} of zipcodes file has a blank zipcode column`);
      continue;
    }
    const geo = await client.geocode({
      params: {
        address: zip,
        key: env.GOOGLE_MAPS_API_KEY,
      },
    });
    const postalResult = geo.data.results.find((result) =>
      result.types.some((type) =>
        new Array<AddressType>(
          AddressType.postal_code,
          AddressType.postal_town
        ).includes(type)
      )
    );
    if (!postalResult) {
      logger.warn(`Zipcode ${zip} returned no geocoding results`);
      continue;
    }

    let nextToken;
    const baseParams = {
      location: postalResult.geometry.location,
      radius: postalResult.geometry.bounds
        ? haversine(postalResult.geometry.bounds) / 2 // Radius of the circumscribing circle
        : 15000,
      type: PlaceType1.cafe,
      query: "Dunkin Donuts",
      key: env.GOOGLE_MAPS_API_KEY,
    };
    do {
      const params = nextToken
        ? { ...baseParams, pagetoken: nextToken }
        : baseParams;
      const textSearchResult = await client.textSearch({
        params,
      });
      yield* textSearchResult.data.results;
      nextToken = textSearchResult.data.next_page_token;
    } while (nextToken);
    rowNum++;
  }
};

async function* getZips(zipsFile: string): AsyncIterable<ZipsFileRow> {
  const parser = fs.createReadStream(zipsFile).pipe(
    parse({
      quote: '"',
      skip_empty_lines: true,
      columns: true,
    })
  );
  yield* parser;
}

const run = async (): Promise<void> => {
  const zipsFile = process.argv[2];
  const outputFile =
    process.argv[3] ?? path.join(`${__dirname}`, "data", "places.json");
  if (typeof zipsFile !== "string") {
    logger.error(
      "Usage: npx ts-node exportDunkinPlaces path/to/zipcodes.csv [path/to/places.json]"
    );
    return process.exit(1);
  }
  await fsp.writeFile(
    outputFile,
    "[\n",
    { encoding: 'utf-8', flag: 'w' }
  );
  let rowNum = 0;
  const placeIds = new Set<string>();
  await pipeline(
    () => getZips(zipsFile),
    (rows) => getPlacesFromZips(rows),
    tap(
      async (place): Promise<void> => {
        const comma = rowNum === 0 ? '' : ',';
        if (place.place_id) {
          if (!placeIds.has(place.place_id)) {
            placeIds.add(place.place_id);
            await fsp.writeFile(
              outputFile,
              `${comma}${JSON.stringify(place)}\n`,
              { encoding: 'utf-8', flag: 'a' }
            );
            rowNum++;
          }
        } else {
          logger.warn(place, 'Place has no place_id property');
        }
        if (rowNum % 10 === 0) {
          logger.debug(`Wrote ${rowNum} rows to ${outputFile}`);
        }
      }
    ),
    consume
  );
  await fsp.writeFile(outputFile, "]", { encoding: 'utf-8', flag: 'a' });
  logger.info(`Wrote a total of ${rowNum} rows to ${outputFile}`);
};

run();
