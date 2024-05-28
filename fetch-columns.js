require("dotenv").config();
const axios = require("axios").default;

module.exports = {
  fetchColumns: (dbName, tableId) =>
    axios.get(
      `${process.env.KIN_BACKEND_URL}/api/tables/${dbName}/${tableId}/fields`
    ),
};
