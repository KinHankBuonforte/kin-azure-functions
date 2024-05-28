const axios = require("axios");
const {
  initSnowflakeConnection,
  flattenObject,
  insertRecords,
  initTable,
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

  const appointments = [];

  for (const r of leads) {
    const leadAppointments = (r.appointments ?? []).map((ap) => ({
      ...ap,
      leadId: r.leadId,
    }));
    appointments.push(...leadAppointments);
    delete r.appointments;
  }
  const flattenLeadRecords = leads.map((x) => flattenObject(x));

  const leadColumns = await initTable(
    connection,
    "ENZY_KIN",
    "ENZY_LEADS",
    flattenLeadRecords,
    true
  );
  await insertRecords(
    connection,
    flattenLeadRecords,
    "ENZY_LEADS",
    leadColumns
  );

  const flattenedAppointments = appointments.map((x) => flattenObject(x));
  const appointmentColumns = await initTable(
    connection,
    "ENZY_KIN",
    "ENZY_APPOINTMENTS",
    flattenedAppointments,
    true
  );
  await insertRecords(
    connection,
    flattenedAppointments,
    "ENZY_APPOINTMENTS",
    appointmentColumns
  );
};

module.exports = async function (context, myTimer) {
  context.log("Start fetching leads...");
  await fetchLeads();
};
