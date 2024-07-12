const { flattenObject, Snowflake } = require("../snowflake");
const axios = require("axios");

const api = axios.default.create({
  baseURL: "https://enzyhr.com",
  headers: {
    Authorization: "Bearer KIN_DEV_O2h4d88TkQcO4Mm",
  },
});

const fetchLeads = async function (context) {
  const { data } = await api.get("/rest/externalApi/leads");

  if (!data.success || !data.output) {
    context.log("No data");
    return;
  }
  const leads = data.output;
  const snowflake = await Snowflake.create(context, "ENZY_KIN");
  const appointments = [];

  for (const r of leads) {
    const leadAppointments = (r.appointments ?? []).map((ap) => ({
      ...ap,
      leadId: r.leadId,
    }));
    appointments.push(...leadAppointments);
  }
  const flattenLeadRecords = leads.map((x) => flattenObject(x));

  const leadColumns = await snowflake.createOrUpdateTable(
    "ENZY_LEADS",
    flattenLeadRecords,
    null,
    true
  );
  await snowflake.insert(flattenLeadRecords, "ENZY_LEADS", leadColumns, true);

  const flattenedAppointments = appointments.map((x) => flattenObject(x));
  const appointmentColumns = await snowflake.createOrUpdateTable(
    "ENZY_APPOINTMENTS",
    flattenedAppointments,
    null,
    true
  );
  await snowflake.insert(
    flattenedAppointments,
    "ENZY_APPOINTMENTS",
    appointmentColumns,
    true
  );
};

module.exports = async function (context, myTimer) {
  context.log("Start fetching leads...");
  await fetchLeads(context);
};
