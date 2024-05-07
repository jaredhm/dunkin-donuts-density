import { LatLngBounds } from "@googlemaps/google-maps-services-js";

export const degreesToRadians = (deg: number) => Math.PI * (deg / 180);

// https://en.wikipedia.org/wiki/Haversine_formula#Formulation
export const haversine = (bounds: LatLngBounds): number => {
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
