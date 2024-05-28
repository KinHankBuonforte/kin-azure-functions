const { default: axios } = require("axios");
const { executeSql, initSnowflakeConnection } = require("../snowflake");

const api = axios.create({
  baseURL: "https://enerflo.io/api",
});

api.interceptors.request.use((options) => {
  options.params = {
    api_key: process.env.ENERFLO_API_KEY,
  };
  return options;
});

const field_mappings = {
  customer: {
    first_name: "first_name",
    last_name: "last_name",
    setter_user_id: "setter_id",
    agent_user_id: "agent_id",
    status: "status_name",
  },
  appointment: {
    survey_id: "survey_id",
    timezone: "timezone",
    name: "appointment_type",
    appointment_type_status: "APPOINTMENT_STATUS",
    status: "status",
    scheduled_start_time: "start_time",
    scheduled_end_time: "end_time",
    customer_id: "enerflo_customer_id",
    creator_id: "creator_id",
  },
  install: {
    customer_id: "customer_id",
    "customer.name": "customer_name",
    "office.name": "office_name",
    setter_user_id: "setter_user_id",
    "customer.agent_user_id": "customer_agent_user_id",
    system_size: "system_size",
    "cost.system_cost_gross": "cost_system_cost_gross",
    "setter_user.timezone": "setter_user_timezone",
    status_name: "status_name",
  },
};

/**
 *
 * @param {*} event
 */
const handle_webhook = async (event, connection) => {
  const { webhook_event, id } = event.PARAMS;

  const [type, entry] = webhook_event.split("_");

  try {
    if (type === "new") {
      const d = await executeSql(
        connection,
        `SELECT ID FROM ENERFLO_${entry.toUpperCase()}S WHERE ID = ${id}`
      );
      if (d.length) {
        await complete_webhook(event, connection);
      }
    } else if (type === "update") {
      const [d] = await executeSql(
        connection,
        `SELECT * FROM ENERFLO_${entry.toUpperCase()}S WHERE ID = ${id}`
      );
      if (!d) {
        await complete_webhook(event, connection);
        return;
      }
      const api_url =
        entry === "customer"
          ? `/v3/customers/${id}`
          : entry === "appointment"
          ? `/v3/customers/${d.ENERFLO_CUSTOMER_ID}/appointments/${id}`
          : `/v3/installs/${id}`;
      if (!api_url) {
        return;
      }
      const { data } = await api.get(api_url);
      const new_data = Object.entries(field_mappings[entry])
        .map(([key, value]) => {
          const nested_keys = key.split(".");

          if (
            nested_keys[0] === "scheduled_start_time" ||
            nested_keys[0] === "scheduled_end_time"
          ) {
            return `${value} = ${map_value(
              data[nested_keys[0]].date
                .replace(".000000", "+00:00")
                .replace(" ", "T")
            )}`;
          }

          return `${value} = ${map_value(
            nested_keys.length === 2
              ? data[nested_keys[0]][[nested_keys[1]]]
              : data[nested_keys[0]]
          )}`;
        })
        .join(", ");
      await executeSql(
        connection,
        `UPDATE ENERFLO_${entry.toUpperCase()}S SET ${new_data} WHERE ID = ${id}`
      );
      await complete_webhook(event, connection);
    } else if (type === "delete") {
      await executeSql(
        connection,
        `DELETE FROM ENERFLO_${entry.toUpperCase()}S WHERE ID = ${id}`
      );
      await complete_webhook(event, connection);
    }
  } catch (err) {
    console.error(`${webhook_event}: ${id}`);
    throw err;
  }
};

const complete_webhook = async (event, connection) => {
  await executeSql(
    connection,
    `UPDATE ENERFLO_WEBHOOK_EVENTS_NEW SET PROCESSED = TRUE WHERE ID = ${event.ID}`
  );
};

const map_value = (value) => {
  if (typeof value == "string") {
    return `'${value.replace(/'/g, '"')}'`;
  } else if (typeof value == "number") {
    return value.toString();
  } else if (!value) {
    return "NULL";
  } else if (typeof value == "object") {
    return `'${JSON.stringify(value)}'`;
  } else {
    return value.toString();
  }
};

const processWebhooks = async function (context, myTimer) {
  var timeStamp = new Date().toISOString();

  context.log("JavaScript timer trigger function ran!", timeStamp);

  const connection = await initSnowflakeConnection();
  const events = await executeSql(
    connection,
    "SELECT * FROM ENERFLO_WEBHOOK_EVENTS_NEW WHERE PROCESSED = FALSE ORDER BY ID ASC"
  );
  for (const e of events) {
    try {
      e.PARAMS = JSON.parse(e.PARAMS.replace(/\s+/g, " "));
      const { webhook_event, id } = e.PARAMS;
      console.log(`${e.ID}: ${webhook_event} - ${id}`);
      await handle_webhook(e, connection);
    } catch (err) {
      console.error("Failed to parse event:", e.ID);
    }
  }
};

module.exports = processWebhooks;
