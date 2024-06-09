const { Snowflake } = require("../snowflake");

const axios = require("axios").default;

module.exports = async function (context, myTimer) {
  const timeStamp = new Date().toISOString();

  if (myTimer.isPastDue) {
    context.log("JavaScript is running late!");
  }
  context.log("JavaScript timer trigger function ran!", timeStamp);
  const snowflake = await Snowflake.create(context);
  const appointmentIds = await snowflake.execute(
    `SELECT ID from ENERFLO_APPOINTMENTS WHERE APPOINTMENT_STATUS IS NULL ORDER BY ID desc`
  );
  context.log(appointmentIds.length);
  let i = 0;
  for (let appointmentId of appointmentIds) {
    console.log(
      `${new Date()}. Running ${appointmentId["ID"]}. ${i} out o  ${
        appointmentIds.length
      }`
    );
    let data = await axios(
      `https://enerflo.io/api/v1/appointments/${appointmentId["ID"]}?api_key=13686046e8dc420946.70185370`,
      { method: "PUT" }
    );
    let appointmentStatus = "Not found";
    if (data.status != 404) {
      const jsonData = data.data;
      if (jsonData["appointment_status"]) {
        appointmentStatus = jsonData["appointment_status"];
      }
    }
    await snowflake.execute(
      `UPDATE ENERFLO_APPOINTMENTS SET APPOINTMENT_STATUS=\'${appointmentStatus}\' where ID=${appointmentId["ID"]}`
    );
    i = i + 1;
  }

  context.res = {
    status: 200 /* Defaults to 200 */,
  };
};

// module.exports(console, {});
