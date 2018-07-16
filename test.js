const Connection = require("database-js").Connection;
const lsdb = require(".");

var conn = new Connection("database-js-localstorage:///tests", lsdb);

(async function() {
    let stmt = conn.prepareStatement("SELECT states.State, abbr.Abbr, states.Ranking, states.Population FROM states JOIN abbr ON states.State = abbr.State WHERE states.Ranking < 11");
    // let stmt = conn.prepareStatement("SELECT * FROM states WHERE Ranking < 11");
    console.log(await stmt.query());
})();