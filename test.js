const Connection = require("database-js").Connection;
const lsdb = require(".");

var conn = new Connection("database-js-localstorage:///tests", lsdb);

(async function() {
    let stmt = conn.prepareStatement("SELECT states.State, visits.Year FROM states FULL JOIN visits ON states.State = visits.State");
    console.log(await stmt.query());
})();