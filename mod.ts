/**
 * @file mod.ts
 * @description Deno script to merge Accident, Vehicle, Passenger, and Pedestrian data from CSV files into a single JSON file using a memory-efficient streaming approach.
 * @version 5.1.0
 *
 * @requires https://deno.land/std/fs/mod.ts
 * @requires https://deno.land/std/csv/mod.ts
 *
 * To Run:
 * 1. Ensure you have the .csv files created by the conversion script.
 * 2. Make sure this script (`mod.ts`) is in the directory with the four .csv files.
 * 3. Open your terminal and run the command (no memory flags needed):
 * deno run --allow-read --allow-write mod.ts
 * 4. A `merged.json` file will be created.
 */

import * as fs from "https://deno.land/std@0.224.0/fs/mod.ts";
import {
  CsvParseStream,
  parse,
} from "https://deno.land/std@0.224.0/csv/mod.ts";

// --- TYPE DEFINITIONS ---
interface IdNamePair {
  id: number | string;
  name: string;
}
interface Passenger {
  [key: string]: any;
}
interface Pedestrian {
  [key: string]: any;
}
interface Vehicle {
  [key: string]: any;
  passengerDTOS?: Passenger[];
}
interface Accident {
  [key: string]: any;
  vehicleDTOS?: Vehicle[];
  pedestrianDTOS?: Pedestrian[];
}

// --- HELPER FUNCTIONS ---

/**
 * Reads a CSV file and converts it into an array of JSON objects.
 * This is used for the smaller "lookup" files. It includes a fix to handle potential
 * Byte Order Mark (BOM) characters that can corrupt the first header.
 * @param filePath The path to the CSV file.
 * @returns A promise that resolves to an array of objects.
 */
async function readCsvFile(filePath: string): Promise<any[]> {
  try {
    let fileContent = await Deno.readTextFile(filePath);
    // FIX: Remove the BOM character from the start of the file if it exists.
    // This prevents the first column header from being corrupted (e.g., becoming "\ufeffaccident_id").
    if (fileContent.startsWith("\ufeff")) {
      fileContent = fileContent.substring(1);
    }

    // Find the first newline character to isolate the header row.
    const firstLineEnd = fileContent.indexOf("\n");
    const firstLine = firstLineEnd === -1
      ? fileContent
      : fileContent.substring(0, firstLineEnd);

    // Parse only the first line to get headers.
    const headers = (await parse(firstLine, { lazyQuotes: true }))[0];
    if (!headers || headers.length === 0) {
      throw new Error(`Could not parse headers from ${filePath}`);
    }

    // Now parse the full content using the extracted headers.
    const records = await parse(fileContent, {
      skipFirstRow: true,
      columns: headers,
      lazyQuotes: true,
    });
    return records;
  } catch (error) {
    console.error(`Error processing CSV file "${filePath}":`, error.message);
    Deno.exit(1);
  }
}

/**
 * Gets the header row from a CSV file for stream parsing.
 * Includes a fix to handle potential Byte Order Mark (BOM) characters.
 * @param filePath The path to the CSV file.
 * @returns A promise that resolves to an array of strings (the headers).
 */
async function getCsvHeaders(filePath: string): Promise<string[]> {
  const file = await Deno.open(filePath, { read: true });
  const reader = file.readable.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += value;
    const endOfLine = buffer.indexOf("\n");
    if (endOfLine !== -1) {
      buffer = buffer.substring(0, endOfLine);
      break;
    }
  }
  file.close();

  // FIX: Remove the BOM character from the buffer before parsing headers.
  if (buffer.startsWith("\ufeff")) {
    buffer = buffer.substring(1);
  }

  const headers = (await parse(buffer, { lazyQuotes: true }))[0];
  if (!headers) throw new Error(`Could not read headers from ${filePath}`);
  return headers;
}

// --- DATA FORMATTING FUNCTIONS ---
// These functions transform flat data rows into the required nested JSON structure.

function formatAccident(a: any): Accident {
  return {
    road: { id: a.road_id || "", name: a.road_name || "" },
    seri: a.seri,
    type: { id: a.type_id || "", name: a.type_name || "" },
    officer: a.officer,
    position: { id: a.position_id || "", name: a.position_name || "" },
    province: { id: a.province_id || "", name: a.province_name || "" },
    serialNO: a.serialNO,
    township: { id: a.township_id || "", name: a.township_name || "" },
    deadCount: a.deadCount,
    xPosition: a.xPosition,
    yPosition: a.yPosition,
    areaUsages: a.areaUsages,
    hasWitness: a.hasWitness,
    newsNumber: a.newsNumber,
    rulingType: { id: a.rulingType_id || "", name: a.rulingType_name || "" },
    airStatuses: a.airStatuses,
    attachments: a.attachments,
    lightStatus: { id: a.lightStatus_id || "", name: a.lightStatus_name || "" },
    roadDefects: a.roadDefects,
    accidentDate: a.accidentDate,
    base64Images: a.base64Images,
    humanReasons: a.AMEL_humanReasons,
    injuredCount: a.injuredCount,
    collisionType: {
      id: a.collisionType_id || "",
      name: a.collisionType_name || "",
    },
    roadSituation: {
      id: a.roadSituation_id || "",
      name: a.roadSituation_name || "",
    },
    completionDate: a.completionDate,
    roadRepairType: {
      id: a.roadRepairType_id || "",
      name: a.roadRepairType_name || "",
    },
    shoulderStatus: {
      id: a.shoulderStatus_id || "",
      name: a.shoulderStatus_name || "",
    },
    vehicleReasons: a.vehicleReasons,
    equipmentDamages: a.equipmentDamages,
    roadSurfaceConditions: a.roadSurfaceConditions,
    accident_id: a.accident_id,
  };
}

function formatVehicle(v: any): Vehicle {
  return {
    color: { id: v.color_id || "", name: v.color_name || "" },
    driver: {
      sex: { id: v.driver_sex_id || "", name: v.driver_sex_name || "" },
      lastName: v.driver_lastName,
      firstName: v.driver_firstName,
      injuryType: {
        id: v.driver_injuryType_id || "",
        name: v.driver_injuryType_name || "",
      },
      licenceType: { name: v.driver_licenceType_name || "" },
      totalReason: {
        id: v.driver_totalReason_id || "",
        name: v.driver_totalReason_name || "",
      },
      nationalCode: v.driver_nationalCode,
      licenceNumber: v.driver_licenceNumber,
    },
    system: { id: v.system_id || "", name: v.system_name || "" },
    plaqueNo: v.plaqueNo,
    plaqueType: { id: v.plaqueType_id || "", name: v.plaqueType_name || "" },
    systemType: { id: v.systemType_id || "", name: v.systemType_name || "" },
    faultStatus: { id: v.faultStatus_id || "", name: v.faultStatus_name || "" },
    insuranceCo: { id: v.insuranceCo_id || "", name: v.insuranceCo_name || "" },
    insuranceNo: v.insuranceNo,
    plaqueUsage: { id: v.plaqueUsage_id || "", name: v.plaqueUsage_name || "" },
    printNumber: v.printNumber,
    plaqueSerial: v.plaqueSerial,
    insuranceDate: v.insuranceDate,
    bodyInsuranceCo: {
      id: v.bodyInsuranceCo_id || "",
      name: v.bodyInsuranceCo_name || "",
    },
    bodyInsuranceNo: v.bodyInsuranceNo,
    motionDirection: {
      id: v.motionDirection_id || "",
      name: v.motionDirection_name || "",
    },
    bodyInsuranceDate: v.bodyInsuranceDate,
    maxDamageSections: v.maxDamageSections,
    damageSectionOther: v.damageSectionOther,
    insuranceWarrantyLimit: v.insuranceWarrantyLimit,
    accident_id: v.accident_id,
    vehicle_id: v.vehicle_id,
  };
}

function formatPassenger(p: any): Passenger {
  return {
    sex: { id: p.sex_id || "", name: p.sex_name || "" },
    lastName: p.lastName,
    firstName: p.firstName,
    injuryType: { id: p.injuryType_id || "", name: p.injuryType_name || "" },
    faultStatus: { id: p.faultStatus_id || "", name: p.faultStatus_name || "" },
    totalReason: { id: p.totalReason_id || "", name: p.totalReason_name || "" },
    nationalCode: p.nationalCode,
  };
}

function formatPedestrian(p: any): Pedestrian {
  return {
    sex: { id: p.sex_id || "", name: p.sex_name || "" },
    lastName: p.lastName,
    firstName: p.firstName,
    injuryType: { id: p.injuryType_id || "", name: p.injuryType_name || "" },
    faultStatus: { id: p.faultStatus_id || "", name: p.faultStatus_name || "" },
    totalReason: { id: p.totalReason_id || "", name: p.totalReason_name || "" },
    nationalCode: p.nationalCode,
  };
}

// --- MAIN EXECUTION LOGIC ---

async function main() {
  console.log("Starting data merging process from CSV files (streaming)...");

  const fileNames = {
    accidents: "Accident1000.csv",
    vehicles: "filtered_vehicle_data_for_test.csv",
    passengers: "passenger.csv",
    pedestrians: "Pedestrian.csv",
  };

  for (const fileName of Object.values(fileNames)) {
    if (!(await fs.exists(fileName))) {
      console.error(`FATAL: Required CSV file not found: "${fileName}".`);
      console.error("Please run the `convert_to_csv.ts` script first.");
      return;
    }
  }
  console.log("All required CSV files found.");

  // Load lookup data into memory. This is necessary for fast merging.
  console.log("Loading lookup data into memory...");
  const vehiclesData = await readCsvFile(fileNames.vehicles);
  const passengersData = await readCsvFile(fileNames.passengers);
  const pedestriansData = await readCsvFile(fileNames.pedestrians);

  // Create lookup maps for efficient merging.
  const vehiclesByAccidentId = new Map<string, Vehicle[]>();
  for (const v of vehiclesData) {
    // Use vehicle_id as it contains the accident ID
    const key = String(v.vehicle_id || "").trim();
    if (!key) continue;
    if (!vehiclesByAccidentId.has(key)) vehiclesByAccidentId.set(key, []);
    vehiclesByAccidentId.get(key)!.push(formatVehicle(v));
  }

  const passengersByAccidentId = new Map<string, Passenger[]>();
  for (const p of passengersData) {
    // Use passenger_id as it contains the accident ID
    const key = String(p.passenger_id || "").trim();
    if (!key) continue;
    if (!passengersByAccidentId.has(key)) passengersByAccidentId.set(key, []);
    passengersByAccidentId.get(key)!.push(formatPassenger(p));
  }

  const pedestriansByAccidentId = new Map<string, Pedestrian[]>();
  for (const p of pedestriansData) {
    // Use pedestrian_id as it contains the accident ID
    const key = String(p.pedestrian_id || "").trim();
    if (!key) continue;
    if (!pedestriansByAccidentId.has(key)) pedestriansByAccidentId.set(key, []);
    pedestriansByAccidentId.get(key)!.push(formatPedestrian(p));
  }
  console.log("Lookup data prepared.");

  // Get headers for the main accidents file to configure the stream parser.
  const accidentHeaders = await getCsvHeaders(fileNames.accidents);

  // Open file streams for reading the main accidents file and writing the output.
  const accidentFileStream = await Deno.open(fileNames.accidents, {
    read: true,
  });
  const outputFile = await Deno.create("merged.json");
  const encoder = new TextEncoder();

  console.log("Processing accidents file via stream...");
  try {
    await outputFile.write(encoder.encode("[\n"));
    let isFirst = true;

    // Create a stream that parses the CSV file row by row.
    const accidentStream = accidentFileStream.readable
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(
        new CsvParseStream({
          skipFirstRow: true,
          columns: accidentHeaders,
          lazyQuotes: true,
        }),
      );

    // Process each accident from the stream without loading the whole file.
    for await (const rawAccident of accidentStream) {
      if (!isFirst) {
        await outputFile.write(encoder.encode(",\n"));
      }

      const accident = formatAccident(rawAccident);
      const accidentId = String(accident.accident_id || "").trim();

      const accidentVehicles = vehiclesByAccidentId.get(accidentId) || [];
      const accidentPassengers = passengersByAccidentId.get(accidentId) || [];

      // Distribute passengers among vehicles for this accident
      // If there are passengers but no vehicles, we'll skip the passengers
      // If there are vehicles, distribute passengers evenly or assign to first vehicle
      for (let i = 0; i < accidentVehicles.length; i++) {
        const vehicle = accidentVehicles[i];
        if (i === 0) {
          // Assign all passengers to the first vehicle for simplicity
          vehicle.passengerDTOS = accidentPassengers;
        } else {
          vehicle.passengerDTOS = [];
        }
        delete vehicle.accident_id;
        delete vehicle.vehicle_id;
      }

      accident.vehicleDTOS = accidentVehicles;
      accident.pedestrianDTOS = pedestriansByAccidentId.get(accidentId) || [];
      delete accident.accident_id;

      const finalObject = { accident_json: JSON.stringify(accident) };
      const jsonString = JSON.stringify(finalObject, null, 2);
      await outputFile.write(encoder.encode(jsonString));

      isFirst = false;
    }

    await outputFile.write(encoder.encode("\n]\n"));
    console.log("\n✅ Successfully created 'merged.json'!");
  } catch (error) {
    console.error("Error during file write operation:", error.message);
  } finally {
    // Ensure the output file is always closed.
    outputFile.close();
  }
}

main().catch((err) => {
  console.error("An unexpected error occurred:", err);
});
