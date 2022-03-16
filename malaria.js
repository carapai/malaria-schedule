var cron = require("node-cron");
const { fromPairs, chunk } = require("lodash");
const { default: axios } = require("axios");
const { subMonths, endOfMonth, format } = require("date-fns");
const fs = require("fs");
const csv = require("csv-parser");
const { queryDHIS2, postDHIS2 } = require("./common");
const logger = require("./Logger");

const createSourceApi = (url, username, password) => {
  let baseURL = String(url).endsWith("/") ? `${url}api/` : `${url}/api/`;
  return axios.create({ baseURL, auth: { username, password } });
};

async function downloadData(
  sectionName,
  remoteDataSet,
  api,
  orgUnit,
  startDate,
  endDate
) {
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
}

const processFile = async (sectionName, combos, attributes, facilities) => {
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

const fetchPerDistrict = async (sectionName, mappings, startDate, endDate) => {
  const log = logger(sectionName);
  const mappingIds = String(mappings).split(",");

  const [ouMapping, mappingDetails, aMapping] = await Promise.all([
    queryDHIS2(`dataStore/o-mapping/${mappingIds[0]}`, {}),
    queryDHIS2(`dataStore/agg-wizard/${mappingIds[0]}`, {}),
    queryDHIS2(`dataStore/a-mapping/${mappingIds[0]}`, {}),
  ]);
  const facilities = fromPairs(
    ouMapping.filter((m) => !!m.mapping).map((m) => [m.id, m.mapping])
  );
  const { remoteDataSet, url, username, password } = mappingDetails;

  const api = createSourceApi(url, username, password);

  const {
    data: { organisationUnits },
  } = await api.get("organisationUnits.json", {
    params: { level: 3, paging: false, fields: "id,displayName" },
  });

  const allCombos = await Promise.all(
    mappingIds.map((mappingId) =>
      queryDHIS2(`dataStore/c-mapping/${mappingId}`, {})
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
      await downloadData(
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

      dataValues = await processFile(
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
          postDHIS2(
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

cron.schedule("0 0 */17 * *", () => {
  try {
    const lastMonth = subMonths(new Date(), 1);
    const start = `${lastMonth.getFullYear()}-${String(
      lastMonth.getMonth() + 1
    ).padStart(2, "0")}-01`;
    const end = format(endOfMonth(lastMonth), "yyyy-MM-dd");
    fetchPerDistrict("WxGJzn1IXPn", "WxGJzn1IXPn", start, end).then(() =>
      console.log("Done")
    );
  } catch (error) {
    console.log(error.message);
  }
});
