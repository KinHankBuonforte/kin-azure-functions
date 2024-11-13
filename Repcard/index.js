const { flattenObject, Snowflake } = require("../snowflake");
const axios = require("axios");

const api = axios.default.create({
  baseURL: "https://app.repcard.com/api",
  headers: {
    "x-api-key": "Lvm4WVpNxl59w84ifkcE4nysfWbY4UplDfKwvoNh8p8ocNgM88LUr5Os8A93kGQampI4s5bXytlrK4D017307462216283",
  },
});

// Helper function to calculate a date 30 days ago
const getDefaultDate = () => {
  const date = new Date();
  date.setDate(date.getDate() - 30);
  return date.toISOString().split("T")[0]; // Format as YYYY-MM-DD
};

// Function to fetch the last updated date from REPCARD_CONTACTS
const getLastUpdatedDate = async (snowflake) => {
  const result = await snowflake.query(
    `SELECT MAX(updatedAt) AS lastUpdated FROM REPCARD.PUBLIC.REPCARD_CONTACTS`
  );
  return result[0]?.LASTUPDATED || getDefaultDate(); // Use dynamic default date if no records found
};

// Fetch contacts from the new endpoint using last_updated_from parameter
const fetchContacts = async function (context) {
  const snowflake = await Snowflake.create(context, "REPCARD");
  const lastUpdatedFrom = await getLastUpdatedDate(snowflake);

  const { data } = await api.get(`/customers`, {
    params: {
      last_updated_from: lastUpdatedFrom,
      per_page: 100,
    },
  });

  if (data.status !== 1 || !data.result.data) {
    context.log("No data or unsuccessful status");
    return;
  }

  const contacts = data.result.data;
  const flattenedContacts = contacts.map((contact) => flattenObject(contact));

  // Create or update table for contacts and insert flattened data
  const contactColumns = await snowflake.createOrUpdateTable(
    "REPCARD_CONTACTS",
    flattenedContacts,
    null,
    true
  );
  await snowflake.insert(flattenedContacts, "REPCARD_CONTACTS", contactColumns, true);
};

module.exports = async function (context, myTimer) {
  context.log("Start fetching contacts...");
  await fetchContacts(context);
};
