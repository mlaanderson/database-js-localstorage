const AbstractDriver = require("database-js-sqlparser");
var storage = typeof localStorage === "undefined" ? require('./localstorage') : localStorage
var { parse} = require('node-sqlparser');

class LocalStorageDb extends AbstractDriver {
    /**
     * @param {string} database The database to use
     */
    constructor(database) {
        super();
        this.database = database;
    }

    getDefault(dbtype) {
        switch (dbtype.type) {
            case 'string':
                if (dbtype.length && dbtype.pad) {
                    return "".padEnd(dbtype.length, dbtype.pad);
                }
                return null;
            case 'binary':
                return null;    
            case 'boolean':
                return false;
            case 'integer':
                return null;
            case 'float': 
                return null;
            case 'date':
                return null;
        }
    }

    toValue(dbtype, jsvalue) {
        switch (dbtype.type) {
            case 'string':
                if (!jsvalue) { return null; }
                if (dbtype.length && dbtype.pad) {
                    return jsvalue.toString().substr(0, dbtype.length).padEnd(dbtype.length, dbtype.pad);
                } else if (dbtype.length) {
                    return jsvalue.toString().substr(0, dbtype.length);
                }
                return jsvalue.toString();
            case 'binary':
                return jsvalue ? jsvalue.toString() : null;    
            case 'boolean':
                return !!jsvalue;
            case 'integer':
                return parseInt(jsvalue.toString());
            case 'float': 
                return parseFloat(jsvalue.toString());
            case 'date':
                return jsvalue ? jsvalue.getTime() : null;
        }
    }

    getPrefix(name) {
        var re = /^(?:([^\.]+)\.)?([^\.]+)$/;
        if (re.test(name) == false) { throw `Invalid table name: ${name}`; }
        var match = re.exec(name);
        match[1] = match[1] || "public";
        return `${this.database}.${match[1]}.tables.${match[2]}`;
    }

    getTableDefinition(table) {
        return JSON.parse(storage.getItem(table + ".definition"));
    }

    getRows(table) {
        try {
            return JSON.parse(storage.getItem(table + ".rows"));
        } catch (e) {
            return null;
        }
    }

    getMappedRows(table, namespace = false) {
        let rows = this.getRows(table);
        let def = this.getTableDefinition(table);

        return rows.map(row => {
            var row_data = {};
            for (let key in def) {
                let skey = namespace ? namespace + '.' + key : key;
                row_data[skey] = row[def[key].index];
            }
            return row_data;
        });
    }
 
    create(table, definition) {
        return new Promise((resolve, reject) => {
            // create a new table definition
            let table_prefix = this.getPrefix(table);
            let columns = {};

            for (let field of definition) {
                columns[field.name] = field;
            }
            storage.setItem(table_prefix + ".definition", JSON.stringify(columns));

            resolve(true);
        });
    }

    drop(table) {
        return new Promise((resolve, reject) => {
            let table_prefix = this.getPrefix(table);
            storage.removeItem(table_prefix + ".definition");
            storage.removeItem(table_prefix + ".rows");
            resolve(true);
        });
    }

    remove(table, index) {
        let table_prefix = this.getPrefix(table);
        let rows = this.getRows(table_prefix);

        rows.splice(index, 1);
        storage.setItem(table_prefix + ".rows", JSON.stringify(rows));
        return Promise.resolve(index);
    }

    load(table) {
        let table_prefix = this.getPrefix(table);
        return Promise.resolve(this.getMappedRows(table_prefix));
    }

    store(table, index, row) {
        return new Promise((resolve, reject) => {
            let table_prefix = this.getPrefix(table);
            let rows = this.getRows(table_prefix);
            let table_def = this.getTableDefinition(table_prefix);

            if (index) {
                Object.keys(row).map(k => {
                    rows[index][table_def[k].index] = this.toValue(table_def[k], row[k]);
                });
            } else {
                let rowArray = Object.values(table_def).map(k => null);
                Object.keys(row).map(k => {
                    rowArray[table_def[k].index] = this.toValue(table_def[k], row[k]);
                });
                index = rows.length;
                rows.push(rowArray);
            }

            storage.setItem(table_prefix + ".rows", JSON.stringify(rows));
            
            resolve(index);
        });
    }

    ready() {
        return Promise.resolve(true);
    }

    close() {
        return Promise.resolve(true);
    }
}

module.exports = {
    /**
     * Opens the connection using the connection object.
     * @param {object} connection
     * @returns {LocalStorageDb}
     */
    open: function(connection) {
        return new LocalStorageDb(connection.Database);
    }
};
