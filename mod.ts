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
import moment from "npm:moment-jalaali@0.10.0";

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
    console.error(`Error processing CSV file "${filePath}":`, (error as Error).message);
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

// --- DATE CONVERSION HELPER ---
/**
 * Converts Jalali (Persian/Shamsi) date to Gregorian date
 * @param jalaliDate The Jalali date string (can be in various formats)
 * @returns Gregorian date string in ISO format, or original string if conversion fails
 */
function convertJalaliToGregorian(jalaliDate: string): string {
  if (!jalaliDate || jalaliDate.trim() === "") return "";

  try {
    // Remove any extra whitespace
    let cleanDate = jalaliDate.trim();

    // Check if it's already a Gregorian date (contains year >= 1900)
    const gregorianPattern = /\b(19|20)\d{2}\b/;
    if (gregorianPattern.test(cleanDate)) {
      return cleanDate;
    }

    // Check if it's a valid Jalali date pattern (year between 1300-1500)
    const jalaliPattern = /\b(13|14|15)\d{2}\b/;
    if (!jalaliPattern.test(cleanDate)) {
      return cleanDate; // Return as-is if doesn't look like Jalali date
    }

    // Extract date and time parts
    // Handle formats like "04/11/1398 5: 45: 00.000000 PM"
    let timeString = "";
    let dateOnly = cleanDate;

    const dateTimeMatch = cleanDate.match(/^(\d{1,2}\/\d{1,2}\/\d{4})\s+(.+)$/);
    if (dateTimeMatch) {
      dateOnly = dateTimeMatch[1];
      timeString = dateTimeMatch[2].trim();
    }

    // Try different Jalali date formats
    const formats = [
      'jMM/jDD/jYYYY',  // MM/DD/YYYY format (common in the data)
      'jDD/jMM/jYYYY',  // DD/MM/YYYY format
      'jYYYY/jMM/jDD',  // YYYY/MM/DD format
      'jYYYY-jMM-jDD',  // YYYY-MM-DD format
      'jYYYY/jM/jD',    // YYYY/M/D format
      'jYYYY-jM-jD',    // YYYY-M-D format
      'jM/jD/jYYYY',    // M/D/YYYY format
      'jD/jM/jYYYY'     // D/M/YYYY format
    ];

    for (const format of formats) {
      const momentDate = moment(dateOnly, format);
      if (momentDate.isValid()) {
        // Validate the converted date is reasonable (after 1900, before 2100)
        const year = momentDate.year();
        if (year >= 1900 && year <= 2100) {
          // Convert to JavaScript Date
          let jsDate = momentDate.toDate();

          // If we have time information, try to parse and apply it
          if (timeString) {
            try {
              // Parse time from formats like "5: 45: 00.000000 PM" or "12:30:45 AM"
              const timeMatch = timeString.match(/(\d{1,2}):?\s*(\d{1,2}):?\s*(\d{1,2})(?:\.\d+)?\s*(AM|PM)?/i);
              if (timeMatch) {
                let hours = parseInt(timeMatch[1]);
                const minutes = parseInt(timeMatch[2]) || 0;
                const seconds = parseInt(timeMatch[3]) || 0;
                const ampm = timeMatch[4]?.toUpperCase();

                if (ampm === 'PM' && hours !== 12) hours += 12;
                if (ampm === 'AM' && hours === 12) hours = 0;

                jsDate.setHours(hours, minutes, seconds, 0);
              }
            } catch (e) {
              // If time parsing fails, just use the date part
            }
          }

          // Format as JavaScript toLocaleString format: "Aug 27, 2024, 8:30:00 PM"
          const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                         'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

          const month = months[jsDate.getMonth()];
          const day = jsDate.getDate();
          const year = jsDate.getFullYear();

          let hours = jsDate.getHours();
          const minutes = jsDate.getMinutes().toString().padStart(2, '0');
          const seconds = jsDate.getSeconds().toString().padStart(2, '0');

          const ampm = hours >= 12 ? 'PM' : 'AM';
          hours = hours % 12;
          if (hours === 0) hours = 12;

          return `${month} ${day}, ${year}, ${hours}:${minutes}:${seconds} ${ampm}`;
        }
      }
    }

    // If no format worked, return original
    return cleanDate;
  } catch (error) {
    console.warn(`Failed to convert Jalali date "${jalaliDate}":`, (error as Error).message);
    return jalaliDate;
  }
}

// --- DATA FORMATTING FUNCTIONS ---
// These functions transform flat data rows into the required nested JSON structure.

function formatAccident(a: any): Accident {
  // Helper function to parse numeric value or return 0
  const parseNumeric = (val: any) => {
    if (val === "" || val === null || val === undefined) return 0;
    const parsed = parseFloat(val);
    return isNaN(parsed) ? 0 : parsed;
  };

  // Helper function to parse ID value
  const parseId = (val: any) => {
    if (val === "" || val === null || val === undefined) return 0;
    const parsed = parseInt(val);
    return isNaN(parsed) ? 0 : parsed;
  };

  // Helper function to parse string arrays
  const parseStringArray = (val: any, itemName: string) => {
    if (!val || val === "") return [];
    const items = val.split(",").map((item: string) => item.trim()).filter((item: string) => item !== "");
    return items.map((item: string, index: number) => ({
      id: index + 1,
      name: item
    }));
  };

  return {
    road: { id: parseId(a.road_id), name: a.road_name || "" },
    seri: a.seri || "",
    type: { id: parseId(a.type_id), name: a.type_name || "" },
    officer: a.officer || "",
    position: { id: parseId(a.position_id), name: a.position_name || "" },
    province: { id: parseId(a.province_id), name: a.province_name || "" },
    serialNO: parseNumeric(a.serialNO),
    township: { id: parseId(a.township_id), name: a.township_name || "تهران" },
    deadCount: parseNumeric(a.deadCount),
    xPosition: parseNumeric(a.xPosition),
    yPosition: parseNumeric(a.yPosition),
    areaUsages: parseStringArray(a.areaUsages, "area"),
    hasWitness: a.hasWitness === "true" || a.hasWitness === true,
    newsNumber: a.newsNumber || "",
    rulingType: { id: parseId(a.rulingType_id), name: a.rulingType_name || "" },
    airStatuses: parseStringArray(a.airStatuses, "air"),
    attachments: a.attachments ? (Array.isArray(a.attachments) ? a.attachments : []) : [],
    lightStatus: { id: parseId(a.lightStatus_id), name: a.lightStatus_name || "" },
    roadDefects: parseStringArray(a.roadDefects, "defect"),
    base64Images: a.base64Images ? (Array.isArray(a.base64Images) ? a.base64Images : []) : [],
    humanReasons: parseStringArray(a.AMEL_humanReasons, "reason"),
    injuredCount: parseNumeric(a.injuredCount),
    collisionType: {
      id: parseId(a.collisionType_id),
      name: a.collisionType_name || "",
    },
    roadSituation: {
      id: parseId(a.roadSituation_id),
      name: a.roadSituation_name || "",
    },
    roadRepairType: {
      id: parseId(a.roadRepairType_id),
      name: a.roadRepairType_name || "",
    },
    shoulderStatus: {
      id: parseId(a.shoulderStatus_id),
      name: a.shoulderStatus_name || "",
    },
    vehicleReasons: parseStringArray(a.vehicleReasons, "vehicle"),
    equipmentDamages: parseStringArray(a.equipmentDamages, "damage"),
    roadSurfaceConditions: parseStringArray(a.roadSurfaceConditions, "surface"),
    accidentDate: convertJalaliToGregorian(a.accidentDate || ""),
    completionDate: convertJalaliToGregorian(a.completionDate || ""),
    accident_id: a.accident_id,
  };
}

function formatVehicle(v: any): Vehicle {
  // Helper function to parse numeric value or return 0
  const parseNumeric = (val: any) => {
    if (val === "" || val === null || val === undefined) return 0;
    const parsed = parseFloat(val);
    return isNaN(parsed) ? 0 : parsed;
  };

  // Helper function to parse ID value
  const parseId = (val: any) => {
    if (val === "" || val === null || val === undefined) return 0;
    const parsed = parseInt(val);
    return isNaN(parsed) ? 0 : parsed;
  };

  // Helper function to parse plaque numbers
  const parsePlaqueNo = (val: any) => {
    if (!val || val === "") return [];
    if (Array.isArray(val)) return val;
    return val.toString().split(/[\s,]+/).filter((item: string) => item.trim() !== "");
  };

  // Helper function to parse damage sections
  const parseDamageSections = (val: any) => {
    if (!val || val === "") return [];
    const sections = val.split(",").map((item: string) => item.trim()).filter((item: string) => item !== "");
    return sections.map((section: string, index: number) => ({
      id: index + 1,
      name: section
    }));
  };

  return {
    color: { id: parseId(v.color_id), name: v.color_name || "" },
    driver: {
      sex: { id: parseId(v.driver_sex_id), name: v.driver_sex_name || "" },
      lastName: v.driver_lastName || "",
      firstName: v.driver_firstName || "",
      injuryType: {
        id: parseId(v.driver_injuryType_id),
        name: v.driver_injuryType_name || "",
      },
      licenceType: { id: parseId(v.driver_licenceType_id), name: v.driver_licenceType_name || "" },
      totalReason: {
        id: parseId(v.driver_totalReason_id),
        name: v.driver_totalReason_name || "",
      },
      nationalCode: v.driver_nationalCode || "",
      licenceNumber: v.driver_licenceNumber || "",
    },
    system: { id: parseId(v.system_id), name: v.system_name || "" },
    plaqueNo: parsePlaqueNo(v.plaqueNo),
    plaqueType: { id: parseId(v.plaqueType_id), name: v.plaqueType_name || "" },
    systemType: { id: parseId(v.systemType_id), name: v.systemType_name || "" },
    faultStatus: { id: parseId(v.faultStatus_id), name: v.faultStatus_name || "" },
    insuranceCo: { id: parseId(v.insuranceCo_id), name: v.insuranceCo_name || "" },
    insuranceNo: v.insuranceNo || "",
    plaqueUsage: { id: parseId(v.plaqueUsage_id), name: v.plaqueUsage_name || "" },
    printNumber: v.printNumber || "",
    plaqueSerial: parsePlaqueNo(v.plaqueSerial),
    insuranceDate: convertJalaliToGregorian(v.insuranceDate || ""),
    bodyInsuranceCo: {
      id: parseId(v.bodyInsuranceCo_id),
      name: v.bodyInsuranceCo_name || "",
    },
    bodyInsuranceNo: v.bodyInsuranceNo || "",
    motionDirection: {
      id: parseId(v.motionDirection_id),
      name: v.motionDirection_name || "",
    },
    bodyInsuranceDate: convertJalaliToGregorian(v.bodyInsuranceDate || ""),
    maxDamageSections: parseDamageSections(v.maxDamageSections),
    damageSectionOther: v.damageSectionOther || "",
    insuranceWarrantyLimit: parseNumeric(v.insuranceWarrantyLimit),
    accident_id: v.accident_id,
    vehicle_id: v.vehicle_id,
  };
}

function formatPassenger(p: any): Passenger {
  // Helper function to parse ID value
  const parseId = (val: any) => {
    if (val === "" || val === null || val === undefined) return 0;
    const parsed = parseInt(val);
    return isNaN(parsed) ? 0 : parsed;
  };

  return {
    sex: { id: parseId(p.sex_id), name: p.sex_name || "" },
    lastName: p.lastName || "",
    firstName: p.firstName || "",
    injuryType: { id: parseId(p.injuryType_id), name: p.injuryType_name || "" },
    faultStatus: { id: parseId(p.faultStatus_id), name: p.faultStatus_name || "" },
    totalReason: { id: parseId(p.totalReason_id), name: p.totalReason_name || "" },
    nationalCode: p.nationalCode || "",
  };
}

function formatPedestrian(p: any): Pedestrian {
  // Helper function to parse ID value
  const parseId = (val: any) => {
    if (val === "" || val === null || val === undefined) return 0;
    const parsed = parseInt(val);
    return isNaN(parsed) ? 0 : parsed;
  };

  return {
    sex: { id: parseId(p.sex_id), name: p.sex_name || "" },
    lastName: p.lastName || "",
    firstName: p.firstName || "",
    injuryType: { id: parseId(p.injuryType_id), name: p.injuryType_name || "" },
    faultStatus: { id: parseId(p.faultStatus_id), name: p.faultStatus_name || "" },
    totalReason: { id: parseId(p.totalReason_id), name: p.totalReason_name || "" },
    nationalCode: p.nationalCode || "",
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
      const jsonString = JSON.stringify(finalObject, null, 4);
      await outputFile.write(encoder.encode(jsonString));

      isFirst = false;
    }

    await outputFile.write(encoder.encode("\n]\n"));
    console.log("\n✅ Successfully created 'merged.json'!");
  } catch (error) {
    console.error("Error during file write operation:", (error as Error).message);
  } finally {
    // Ensure the output file is always closed.
    outputFile.close();
  }
}

main().catch((err) => {
  console.error("An unexpected error occurred:", err);
});
