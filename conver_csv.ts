/**
 * @file convert_to_csv.ts
 * @description A helper script to convert XLSX files to CSV format.
 * @version 1.0.0
 * @requires https://deno.land/x/sheetjs/xlsx.mjs
 * @requires https://deno.land/std/path/mod.ts
 * @requires https://deno.land/std/csv/mod.ts
 *
 * To Run:
 * 1. Place this script in the same directory as your four .xlsx files.
 * 2. Run the command: deno run -A convert_to_csv.ts
 * 3. This will create four new .csv files in the same directory.
 */

import * as xlsx from "https://deno.land/x/sheetjs/xlsx.mjs";
import * as path from "https://deno.land/std@0.224.0/path/mod.ts";
import { stringify } from "https://deno.land/std@0.224.0/csv/stringify.ts";

/**
 * Converts a single XLSX file to its CSV equivalent.
 * @param inputPath The path to the .xlsx file.
 */
async function convertXlsxToCsv(inputPath: string) {
  // Create the output filename, e.g., "Accident1000.xlsx" -> "Accident1000.csv"
  const outputName = path.basename(inputPath, ".xlsx") + ".csv";
  console.log(`Converting ${inputPath} to ${outputName}...`);

  try {
    const fileData = await Deno.readFile(inputPath);
    const workbook = xlsx.read(fileData);
    const sheetName = workbook.SheetNames[0];

    if (!sheetName) {
      console.error(`Error: No sheets found in ${inputPath}`);
      return;
    }

    const worksheet = workbook.Sheets[sheetName];
    // Convert sheet to an array of arrays, which is the format needed for CSV stringification.
    const jsonData = xlsx.utils.sheet_to_json<string[]>(worksheet, {
      header: 1,
      defval: "",
    });

    if (jsonData.length === 0) {
      console.warn(
        `Warning: File ${inputPath} appears to be empty. Creating an empty CSV.`,
      );
      await Deno.writeTextFile(outputName, "");
      return;
    }

    // Use Deno's standard library to create a valid CSV string.
    const csvString = await stringify(jsonData);
    await Deno.writeTextFile(outputName, csvString);
    console.log(`✅ Successfully created ${outputName}`);
  } catch (error) {
    console.error(`Failed to process file "${inputPath}":`, error.message);
  }
}

async function main() {
  console.log("--- Starting XLSX to CSV Conversion ---");
  const filesToConvert = [
    "Accident1000.xlsx",
    "filtered_vehicle_data_for_test.xlsx",
    "passenger.xlsx",
    "Pedestrian.xlsx",
  ];

  for (const file of filesToConvert) {
    await convertXlsxToCsv(file);
  }
  console.log(
    "\n--- Conversion process complete. You can now run the main mod.ts script. ---",
  );
}

main();
