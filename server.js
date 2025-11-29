import dotenv from "dotenv";
import { Client } from "pg";

// ScrapeNinja configuration (kept for compatibility with existing setup)
const SCRAPE_NINJA_ENDPOINT = "https://scrapeninja.p.rapidapi.com/scrape";
const SCRAPE_NINJA_HOST = "scrapeninja.p.rapidapi.com";
const DEFAULT_SCRAPE_NINJA_API_KEY =
  "455e2a6556msheffc310f7420b51p102ea0jsn1c531be1e299";

dotenv.config();

const DB_CONFIG = {
  host: process.env.DB_HOST || "3.140.167.34",
  port: Number.parseInt(process.env.DB_PORT || "5432", 10),
  user: process.env.DB_USER || "redash",
  password: process.env.DB_PASSWORD || "te83NECug38ueP",
  database: process.env.DB_NAME || "scrapers",
};

const SOURCE_TABLE = "apple_podcasts.profiles";
const SOURCE_NAME = SOURCE_TABLE.replace(/\.profiles$/, "");

const EMAIL_REGEX = /[\w.+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g;
const PHONE_REGEX = /\+?\d[\d\s().-]{7,}\d/g;
const URL_REGEX = /https?:\/\/[^\s"']+/g;

const normalizeMatch = (match) => match.trim().replace(/[.,;:]+$/, "");

function parseContacts(text) {
  if (!text) return { emails: [], phones: [], urls: [] };

  const content = String(text);

  const emails = new Set();
  const phones = new Set();
  const urls = new Set();

  (content.match(EMAIL_REGEX) || []).forEach((match) =>
    emails.add(normalizeMatch(match))
  );

  (content.match(PHONE_REGEX) || [])
    .map(normalizeMatch)
    .forEach((match) => {
      const digits = match.replace(/\D/g, "");
      if (digits.length >= 7) {
        phones.add(match);
      }
    });

  (content.match(URL_REGEX) || []).forEach((match) =>
    urls.add(normalizeMatch(match))
  );

  return {
    emails: Array.from(emails),
    phones: Array.from(phones),
    urls: Array.from(urls),
  };
}

async function ensureContactsTable(client) {
  await client.query("CREATE SCHEMA IF NOT EXISTS text_parser;");
  await client.query(`
    CREATE TABLE IF NOT EXISTS text_parser.contacts_from_profiles_descriptions (
      id SERIAL PRIMARY KEY,
      source TEXT NOT NULL,
      record_id BIGINT NOT NULL,
      contact_type TEXT NOT NULL,
      contact_value TEXT NOT NULL
    );
  `);
}

async function fetchProfiles(client) {
  const query = `SELECT id, show_description AS input FROM ${SOURCE_TABLE}`;
  const { rows } = await client.query(query);
  return rows;
}

async function saveContacts(client, contacts) {
  if (!contacts.length) return;

  const insertQuery = `
    INSERT INTO text_parser.contacts_from_profiles_descriptions
      (source, record_id, contact_type, contact_value)
    VALUES ($1, $2, $3, $4)
  `;

  for (const { recordId, contactType, contactValue } of contacts) {
    await client.query(insertQuery, [SOURCE_NAME, recordId, contactType, contactValue]);
  }
}

function buildContactRows(records) {
  const contacts = [];

  records.forEach(({ id, input }) => {
    const { emails, phones, urls } = parseContacts(input);

    emails.forEach((email) =>
      contacts.push({
        recordId: id,
        contactType: "email",
        contactValue: email,
      })
    );

    phones.forEach((phone) =>
      contacts.push({
        recordId: id,
        contactType: "phone",
        contactValue: phone,
      })
    );

    urls.forEach((url) =>
      contacts.push({
        recordId: id,
        contactType: "url",
        contactValue: url,
      })
    );
  });

  return contacts;
}

async function main() {
  const client = new Client(DB_CONFIG);
  await client.connect();

  try {
    await ensureContactsTable(client);
    const records = await fetchProfiles(client);
    console.log(`Fetched ${records.length} records from ${SOURCE_TABLE}`);

    const contacts = buildContactRows(records);
    console.log(`Extracted ${contacts.length} contacts`);

    await saveContacts(client, contacts);
    console.log(
      `Saved contacts to text_parser.contacts_from_profiles_descriptions with source ${SOURCE_NAME}`
    );
  } finally {
    await client.end();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error("Failed to extract contacts", error);
    process.exitCode = 1;
  });
}

export {
  SCRAPE_NINJA_ENDPOINT,
  SCRAPE_NINJA_HOST,
  DEFAULT_SCRAPE_NINJA_API_KEY,
  DB_CONFIG,
  SOURCE_TABLE,
  SOURCE_NAME,
  parseContacts,
  buildContactRows,
};
