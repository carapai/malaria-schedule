const { fetchPerDistrict } = require("./common");
const args = process.argv.slice(2);
if (args.length >= 2) {
  const which = args.length === 5 ? 1 : 2;
  fetchPerDistrict(args[0], args[1], args[2], args[3], which).then(() =>
    console.log("Done")
  );
} else {
  console.log("Wrong arguments");
}
