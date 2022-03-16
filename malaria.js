var cron = require("node-cron");
const { subMonths, endOfMonth, format } = require("date-fns");
const { fetchPerDistrict } = require("./common");

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
