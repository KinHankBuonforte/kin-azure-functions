const axios = require("axios");
const {
  initSnowflakeConnection,
  flattenObject,
  insertRecords,
} = require("../snowflake");

const api = axios.default.create({
  baseURL: "https://enzyhr.com",
  headers: {
    Authorization: "Bearer KIN_DEV_O2h4d88TkQcO4Mm",
  },
});

const fetchLeads = async function () {
  const { data } = await api.get("/rest/externalApi/leads");

  if (!data.success || !data.output) {
    context.log("No data");
    return;
  }
  const leads = data.output;
  const connection = await initSnowflakeConnection("ENZY_KIN");
  const flattenRecords = leads.map((x) => flattenObject(x));

  const appointments = [];

  for (const r of flattenRecords) {
    const leadAppointments = (r.appointments ?? []).map((ap) => ({
      ...ap,
      leadId: r.leadId,
    }));
    appointments.push(...leadAppointments);
    delete r.appointments;
  }
  await insertRecords(connection, flattenRecords, "ENZY_LEADS");

  const flattenedAppointments = appointments.map((x) => flattenObject(x));
  await insertRecords(connection, flattenedAppointments, "ENZY_APPOINTMENTS");
};

module.exports = async function (context, myTimer) {
  context.log("Start fetching leads...");
  await fetchLeads();
};
