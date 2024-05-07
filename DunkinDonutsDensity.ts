import {
  AddressComponent,
  AddressType,
  Client,
  Place,
  PlaceType1,
} from "@googlemaps/google-maps-services-js";
import { parser } from 'stream-json';
import { streamArray } from 'stream-json/streamers/StreamArray';
import assert from "assert";
import axios from "axios";
import axiosRetry from "axios-retry";
import { config } from "dotenv";
import path from "path";
import pino, { Level, Logger } from "pino";
import fs, { promises as fsp } from "fs";
import { parse } from "csv-parse";
import { AnyIterable, consume, pipeline, tap } from "streaming-iterables";
import { haversine } from "./utils";
import * as sqlite from "sqlite";
import sqlite3 from "sqlite3";

const PLACES_TABLE = "places";
const ADDRESS_COMPONENTS_TABLE = "address_components";

type Env = {
  LOG_LEVEL: Level;
  GOOGLE_MAPS_API_KEY: string;
};

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

export default class DunkinDonutsDensity {
  private client: Client;
  private env: Env;
  private logger: Logger;
  private db: Promise<sqlite.Database>;

  constructor(public readonly dataDir: string) {
    config();
    this.env = this.getEnv();
    this.logger = pino({
      level: this.env.LOG_LEVEL,
    });
    this.db = sqlite.open({
      filename: path.join(this.dataDir, "sqlite.db"),
      driver: sqlite3.Database,
    });
    axiosRetry(axios, {
      retries: 3,
      retryDelay: axiosRetry.exponentialDelay,
      retryCondition: (err) => (err.response?.status ?? 500) >= 400,
      onMaxRetryTimesExceeded: (error, retry) => {
        this.logger.error(
          error,
          `Request to API failed after ${retry} retries`
        );
      },
    });
    this.client = new Client({ axiosInstance: axios });
  }

  private getEnv(): Env {
    const apiKey = process.env.GOOGLE_MAPS_API_KEY;
    assert(typeof apiKey === "string");
    const logLevel =
      new Array<Level>("fatal", "error", "warn", "info", "debug", "trace").find(
        (logLevel) => logLevel === process.env.LOG_LEVEL
      ) ?? "debug";
    return {
      GOOGLE_MAPS_API_KEY: apiKey,
      LOG_LEVEL: logLevel,
    };
  }

  private async *getPlacesFromZips(
    rows: AnyIterable<ZipsFileRow>
  ): AsyncIterable<Place> {
    let rowNum = 0;
    for await (const row of rows) {
      const { zip } = row;
      if (!zip) {
        this.logger.error(
          `Row ${rowNum} of zipcodes file has a blank zipcode column`
        );
        continue;
      }
      const geo = await this.client.geocode({
        params: {
          address: zip,
          key: this.env.GOOGLE_MAPS_API_KEY,
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
        this.logger.warn(`Zipcode ${zip} returned no geocoding results`);
        continue;
      }

      let nextToken;
      const baseParams = {
        location: postalResult.geometry.location,
        radius: postalResult.geometry.bounds
          ? haversine(postalResult.geometry.bounds) / 1 // Radius of the circumscribing circle
          : 15000,
        type: PlaceType1.cafe,
        query: "Dunkin Donuts",
        key: this.env.GOOGLE_MAPS_API_KEY,
      };
      do {
        const params = nextToken
          ? { ...baseParams, pagetoken: nextToken }
          : baseParams;
        const textSearchResult = await this.client.textSearch({
          params,
        });
        yield* textSearchResult.data.results;
        nextToken = textSearchResult.data.next_page_token;
      } while (nextToken);
      rowNum++;
    }
  }

  private async *getZips(zipsFile: string): AsyncIterable<ZipsFileRow> {
    const parser = fs.createReadStream(zipsFile).pipe(
      parse({
        quote: '"',
        skip_empty_lines: true,
        columns: true,
      })
    );
    yield* parser;
  }

  public async exportDunkinPlaces(
    zipsFile: string,
    outputFile = path.join(this.dataDir, "places.json")
  ): Promise<void> {
    await fsp.writeFile(outputFile, "[\n", { encoding: "utf-8", flag: "w" });
    const placeIds = new Set<string>();
    await pipeline(
      () => this.getZips(zipsFile),
      (rows) => this.getPlacesFromZips(rows),
      tap(async (place): Promise<void> => {
        const comma = placeIds.size === 0 ? "" : ",";
        if (place.place_id) {
          if (!placeIds.has(place.place_id)) {
            placeIds.add(place.place_id);
            await fsp.writeFile(
              outputFile,
              `${comma}${JSON.stringify(place)}\n`,
              { encoding: "utf-8", flag: "a" }
            );
            if (placeIds.size % 10 === 0) {
              this.logger.debug(`Wrote ${placeIds.size} rows to ${outputFile}`);
            }
          }
        } else {
          this.logger.error(place, "Place has no place_id property");
        }
      }),
      consume
    );
    await fsp.writeFile(outputFile, "]", { encoding: "utf-8", flag: "a" });
    this.logger.info(`Wrote a total of ${placeIds.size} rows to ${outputFile}`);
  }

  private async initDb() {
    const db = await this.db;
    await db.run(`DROP TABLE IF EXISTS ${PLACES_TABLE};`);
    await db.run(`DROP TABLE IF EXISTS ${ADDRESS_COMPONENTS_TABLE};`);
    await db.run(`
      CREATE TABLE ${PLACES_TABLE} (
        id INTEGER PRIMARY KEY
        ,place_id TEXT NOT NULL
        ,lat REAL NOT NULL
        ,lng REAL NOT NULL
      );
    `);
    await db.run(`
      CREATE TABLE ${ADDRESS_COMPONENTS_TABLE} (
        id INTEGER PRIMARY KEY
        ,place_id INTEGER NOT NULL
        ,long_name TEXT
        ,short_name TEXT
        ,type TEXT
      );
    `);
  }

  private async insert(place: Place, addressComponents: Array<AddressComponent>) {
    const placeId = place.place_id;
    const lat = place.geometry?.location.lat;
    const lng = place.geometry?.location.lng;

    const db = await this.db;
    const insertResult = await db.run(
      `
        INSERT INTO ${PLACES_TABLE}
          (
            place_id
            ,lat
            ,lng
          )
          VALUES
          (
            :place_id
            ,:lat
            ,:lng
          );
      `,
      {
        ":place_id": placeId,
        ":lat": lat,
        ":lng": lng,
      }
    );

    for (const address of addressComponents) {
      for (const type of address.types) {
        await db.run(
          `
            INSERT INTO ${ADDRESS_COMPONENTS_TABLE}
              (
                place_id
                ,long_name
                ,short_name
                ,type 
              )
              VALUES
              (
                :place_id
                ,:long_name
                ,:short_name
                ,:type
              );
          `,
          {
            ":place_id": insertResult.lastID,
            ":long_name": address.long_name,
            ":short_name": address.short_name,
            ":type": type
          }
        );
      }
    }
  }

  public async geocodeDunkinDonuts(placesFilename: string): Promise<void> {
    await this.initDb();
    const objects = fs.createReadStream(path.join(this.dataDir, placesFilename))
      .pipe(parser())
      .pipe(streamArray());
    objects.on(
      'data',
      (object: { value: Place }) => {
        this.client.geocode({
          params: {
            place_id: object.value.place_id,
            key: this.env.GOOGLE_MAPS_API_KEY
          }
        }).then((geocodeResponse) => {
          this.insert(
            object.value,
            geocodeResponse.data.results[0].address_components
          )
        })
      }
    );
    await new Promise((resolve) => objects.on('end',resolve));
  }
}
