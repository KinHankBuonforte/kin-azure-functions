const fs = require("fs");
const { default: axios } = require("axios");
const { Snowflake, sqlString } = require("../snowflake");

async function addExternalId(records, context = console) {
  try {
    for (let i = 0; i < records.length; i++) {
      let retry = 3;

      while (retry > 0) {
        const externalId = await getExternalId(records[i].id, context);

        if (externalId !== null) {
          retry -= 1;
          await new Promise((resolve) => setTimeout(resolve, 10000));
        } else {
          chunk[j].lead_integration_record_id = externalId;
          break;
        }
      }
    }
  } catch (err) {
    context.error(err?.response?.data);
  }
}

async function getExternalId(enerfloV2Id, context = console) {
  try {
    const {
      data: { data },
    } = await axios.post(
      "https://enerflo.io/api/v1/integrations/map/search?v=v1v2",
      {
        integration_record_type: "EnerfloV2Customer",
        integration_id: "35",
        integration_record_id: enerfloV2Id,
      },
      {
        headers: {
          "api-key": process.env.ENERFLO_API_KEY,
        },
      }
    );
    try {
      const isEnzy = data.iLead.lead_source === "enzy";

      return isEnzy
        ? data.iLead.integrations.Partner.Lead.integration_record_id
        : "";
    } catch (err) {
      return "";
    }
  } catch (err) {
    context.error(enerfloV2Id, err?.response?.data);
    return null;
  }
}

async function fetchExternalIDsForExistingRecords() {
  const snowflake = await Snowflake.create();
  const tableName = "ENERFLO_CUSTOMERS";
  const allRecords = await snowflake.execute(
    `SELECT id FROM ${tableName} WHERE lead_integration_record_id IS NULL order by inserted_at desc`
  );

  while (1) {
    let records = [...allRecords].map((r) => ({
      id: r.ID,
    }));
    const existingRecords = JSON.parse(
      fs.readFileSync("./enerflo-customers.json", "utf8") ?? "[]"
    );
    records = records.filter(
      (r) => !existingRecords.find((er) => er.id === r.id)
    );
    await addExternalId(records);
    records = records.filter((r) => "lead_integration_record_id" in r);
    const all = [...existingRecords, ...records];
    console.log(all[all.length - 1], allRecords.length, all.length);
    fs.writeFileSync("./enerflo-customers.json", JSON.stringify(all), "utf8");

    if (all.length === allRecords.length) {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 10000));
  }
}

async function updateExistingRecords() {
  const snowflake = await Snowflake.create();
  const tableName = "ENERFLO_CUSTOMERS";

  let records = JSON.parse(
    fs.readFileSync("./enerflo-customers.json", "utf8") ?? "[]"
  );

  records = records.map((r) => ({
    ...r,
    lead_integration_record_id: sqlString(
      r.lead_integration_record_id,
      "VARCHAR"
    ),
  }));

  const chunkSize = 200;

  for (let i = 0; i < records.length; i += chunkSize) {
    const chunk = records.slice(i, i + chunkSize);
    const query = chunk
      .map(
        (r) =>
          `UPDATE ${tableName} SET lead_integration_record_id = ${
            r.lead_integration_record_id ?? null
          } WHERE ID = '${r.id}'`
      )
      .join("; ");

    console.log(i);

    try {
      await snowflake.execute(query, { MULTI_STATEMENT_COUNT: chunk.length });
    } catch (err) {
      console.error(err);
    }
  }
}

module.exports = {
  addExternalId,
};

// updateExistingRecords();
