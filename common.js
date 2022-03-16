const axios = require("axios");
const csv = require("csv-parser");
const fs = require("fs");
const FormData = require("form-data");
const { fromPairs, chunk } = require("lodash");
const logger = require("./Logger");
const dotenv = require("dotenv");

const result = dotenv.config();

if (result.error) {
  throw result.error;
}
module.exports.getDHIS2Url1 = (uri) => {
  if (uri !== "") {
    try {
      const url = new URL(uri);
      const dataURL = url.pathname.split("/");
      const apiIndex = dataURL.indexOf("api");

      if (apiIndex !== -1) {
        return url.href;
      } else {
        if (dataURL[dataURL.length - 1] === "") {
          return url.href + "api";
        } else {
          return url.href + "/api";
        }
      }
    } catch (e) {
      console.log(e.message);
    }
  }
  return null;
};

module.exports.createDHIS2Auth = () => {
  const username = process.env.DHIS2_USER;
  const password = process.env.DHIS2_PASS;
  return { username, password };
};

module.exports.getDHIS2Url = () => {
  const uri = process.env.DHIS2_URL;
  return this.getDHIS2Url1(uri);
};

module.exports.queryDHIS2 = async (path, params) => {
  try {
    const baseUrl = this.getDHIS2Url();
    if (baseUrl) {
      const urlx = `${baseUrl}/${path}`;
      const { data } = await axios.get(urlx, {
        auth: this.createDHIS2Auth(),
        params,
      });
      return data;
    }
  } catch (e) {
    console.log(e.message);
  }
};

module.exports.postDHIS2 = async (path, postData, params) => {
  try {
    const baseUrl = this.getDHIS2Url();
    if (baseUrl) {
      const urlx = `${baseUrl}/${path}`;
      const { data } = await axios.post(urlx, postData, {
        auth: this.createDHIS2Auth(),
        params,
      });
      return data;
    }
  } catch (e) {
    console.log(e.message);
  }
};

module.exports.deleteDHIS2 = async (path) => {
  try {
    const baseUrl = getDHIS2Url();
    if (baseUrl) {
      const urlx = `${baseUrl}/${path}`;
      const { data } = await axios.delete(urlx, {
        auth: this.createDHIS2Auth(),
      });
      return data;
    }
  } catch (e) {
    console.log(e.message);
  }
};

module.exports.updateDHIS2 = async (path, postData, params) => {
  try {
    const baseUrl = getDHIS2Url();
    if (baseUrl) {
      const urlx = `${baseUrl}/${path}`;
      const { data } = await axios.put(urlx, postData, {
        auth: this.createDHIS2Auth(),
        params,
      });
      return data;
    }
  } catch (e) {
    console.log(e.message);
  }
};

module.exports.uploadDHIS2 = async (path, file, fileName, params) => {
  try {
    const baseUrl = getDHIS2Url();
    if (baseUrl) {
      const urlx = `${baseUrl}/${path}`;
      const form = new FormData();
      form.append(fileName, file, `${fileName}.csv`);
      const { data } = await axios.post(urlx, form, {
        auth: this.createDHIS2Auth(),
        params,
        headers: form.getHeaders(),
      });
      return data;
    }
  } catch (e) {
    console.log(e.message);
  }
};

module.exports.validText = (dataType, value) => {
  switch (dataType) {
    case "TEXT":
    case "LONG_TEXT":
      return !!value;
    case "NUMBER":
      return !isNaN(Number(value));
    case "EMAIL":
      const re = /\S+@\S+\.\S+/;
      return re.test(String(value).toLowerCase());
    case "BOOLEAN":
      return (
        String(value).toLowerCase() === "false" ||
        String(value).toLowerCase() === "true"
      );
    case "TRUE_ONLY":
      return String(value).toLowerCase() === "true";
    case "PERCENTAGE":
      return Number(value) >= 0 && Number(value) <= 100;
    case "INTEGER":
      return !isNaN(Number(value)) && Number.isInteger(Number(value));
    case "UNIT_INTERVAL":
      return Number(value) >= 0 && Number(value) <= 1;
    case "INTEGER_NEGATIVE":
    case "NEGATIVE_INTEGER":
      return Number.isInteger(Number(value)) && Number(value) < 0;
    case "INTEGER_ZERO_OR_POSITIVE":
    case "AGE":
      const v = Number(value);
      return !isNaN(v) && Number.isInteger(v) && v >= 0;
    case "COORDINATE":
      try {
        const c = JSON.parse(value);
        return _.isArray(c) && c.length === 2;
      } catch (e) {
        return false;
      }
    default:
      return true;
  }
};

module.exports.validateValue = (dataType, value, optionSetValue, optionSet) => {
  if (optionSetValue && !!value) {
    const options = optionSet.options.map((o) => {
      return {
        code: o.code,
        value: o.value,
      };
    });
    const coded = options.find((o) => {
      return (
        String(value).toLowerCase() === String(o.code).toLowerCase() ||
        String(value).toLowerCase() === String(o.value).toLowerCase()
      );
    });
    if (!!coded) {
      return coded.code;
    }
  } else if (!!value && this.validText(dataType, value)) {
    if ((dataType === "BOOLEAN") | (dataType === "TRUE_ONLY")) {
      return String(value).toLowerCase();
    }
    return value;
  }
  return null;
};

module.exports.readCSV = (fileName) => {
  const results = [];
  return new Promise((resolve, reject) => {
    fs.createReadStream(fileName)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => {
        resolve(results);
      });
  });
};

module.exports.createSourceApi = (url, username, password) => {
  let baseURL = String(url).endsWith("/") ? `${url}api/` : `${url}/api/`;
  return axios.default.create({ baseURL, auth: { username, password } });
};

module.exports.downloadData = async (
  sectionName,
  remoteDataSet,
  api,
  orgUnit,
  startDate,
  endDate
) => {
  const response = await api({
    url: "dataValueSets.csv",
    method: "GET",
    responseType: "stream",
    params: {
      dataSet: remoteDataSet,
      startDate,
      endDate,
      orgUnit,
      children: true,
    },
  });
  response.data.pipe(fs.createWriteStream(`${sectionName}.csv`));
  return new Promise((resolve, reject) => {
    response.data.on("end", () => {
      resolve();
    });
    response.data.on("error", () => {
      reject();
    });
  });
};

module.exports.processFile = async (
  sectionName,
  combos,
  attributes,
  facilities
) => {
  const dataValues = [];
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(`${sectionName}.csv`);
    const parser = csv();
    stream.on("ready", () => {
      stream.pipe(parser);
    });
    parser.on("readable", function () {
      let row;
      while ((row = parser.read())) {
        const {
          dataelement: de,
          period,
          categoryoptioncombo: coc,
          attributeoptioncombo: aoc,
          orgunit: unit,
          value,
        } = row;
        const deAndCoc = combos[`${de},${coc}`];
        const orgUnit = facilities[unit];
        const attributeOptionCombo = attributes[aoc];
        const [dataElement, categoryOptionCombo] = String(deAndCoc).split(",");
        if (
          deAndCoc &&
          orgUnit &&
          attributeOptionCombo &&
          dataElement &&
          categoryOptionCombo
        ) {
          dataValues.push({
            dataElement,
            period,
            orgUnit,
            categoryOptionCombo,
            attributeOptionCombo,
            value,
          });
        }
      }
    });

    parser.on("error", function (err) {
      console.error(err.message);
      reject();
    });

    parser.on("end", function () {
      resolve(dataValues);
    });
  });
};

module.exports.fetchPerDistrict = async (
  sectionName,
  mappings,
  startDate,
  endDate
) => {
  const log = logger(sectionName);
  const mappingIds = String(mappings).split(",");

  const [ouMapping, mappingDetails, aMapping] = await Promise.all([
    this.queryDHIS2(`dataStore/o-mapping/${mappingIds[0]}`, {}),
    this.queryDHIS2(`dataStore/agg-wizard/${mappingIds[0]}`, {}),
    this.queryDHIS2(`dataStore/a-mapping/${mappingIds[0]}`, {}),
  ]);
  const facilities = fromPairs(
    ouMapping.filter((m) => !!m.mapping).map((m) => [m.id, m.mapping])
  );
  const { remoteDataSet, url, username, password } = mappingDetails;

  const api = this.createSourceApi(url, username, password);

  const {
    data: { organisationUnits },
  } = await api.get("organisationUnits.json", {
    params: { level: 3, paging: false, fields: "id,displayName" },
  });

  const allCombos = await Promise.all(
    mappingIds.map((mappingId) =>
      this.queryDHIS2(`dataStore/c-mapping/${mappingId}`, {})
    )
  );
  let combos = {};

  allCombos.forEach((combo) => {
    combos = {
      ...combos,
      ...fromPairs(
        combo.filter((m) => !!m.mapping).map((m) => [m.id, m.mapping])
      ),
    };
  });
  const attributes = fromPairs(
    aMapping.filter((m) => !!m.mapping).map((m) => [m.id, m.mapping])
  );

  for (const district of organisationUnits) {
    try {
      log.info(
        `Downloading data for ${
          district.displayName
        } for mappings (${mappingIds.join(",")})`
      );
      await this.downloadData(
        sectionName,
        remoteDataSet,
        api,
        district.id,
        startDate,
        endDate
      );
      log.info(
        `Processing data for ${
          district.displayName
        } for mappings (${mappingIds.join(",")})`
      );
      let dataValues = [];

      dataValues = await this.processFile(
        sectionName,
        combos,
        attributes,
        facilities
      );

      if (dataValues.length > 0) {
        log.info(
          `Inserting ${dataValues.length} records for ${district.displayName}`
        );
        const requests = chunk(dataValues, 50000).map((dvs) =>
          this.postDHIS2(
            "dataValueSets",
            { dataValues: dvs },
            {
              async: true,
              dryRun: false,
              strategy: "NEW_AND_UPDATES",
              preheatCache: true,
              skipAudit: true,
              dataElementIdScheme: "UID",
              orgUnitIdScheme: "UID",
              idScheme: "UID",
              skipExistingCheck: false,
              format: "json",
            }
          )
        );
        const responses = await Promise.all(requests);
        for (const response of responses) {
          log.info(`Created task with id ${response.response.id}`);
        }
      }
    } catch (error) {
      log.error(error.message);
    }
  }
};
