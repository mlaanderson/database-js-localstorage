var storage = typeof localStorage === "undefined" ? require('./localstorage') : localStorage
var { parse} = require('node-sqlparser');

class LocalStorageDb {
    /**
     * @param {string} database The database to use
     */
    constructor(database) {
        this.database = database;
    }

    typeMap(dbtype) {
        switch (dbtype.toUpperCase()) {
            case 'CHAR':
            case 'CHARACTER':
                return 'string';
            case 'VARCHAR':
                return 'string';
            case 'BINARY':
            case 'VARBINARY':
                return 'string';
            case 'BOOLEAN':
                return 'boolean';
            case 'INTEGER':
            case 'SMALLINT':
            case 'BIGINT':                
            case 'DECIMAL':
            case 'NUMERIC':
            case 'FLOAT':
            case 'REAL':
            case 'DOUBLE':
                return 'number';
            case 'DATE':
            case 'TIME':
            case 'TIMESTAMP':
                return 'date';
            case 'INTERVAL':
            case 'ARRAY':
            case 'MULTISET':
            case 'XML':
                throw dbtype.toUpperCase() + ' not yet supported';
            case 'TEXT':
                return 'string';
        }

        return null;
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

    fromValue(dbtype, dbvalue) {
        switch(dbtype.type) {
            case 'string':
                return dbvalue ? dbvalue.toString() : null;
            case 'binary':
                return null;
            case 'boolean':
                return !!dbvalue;
            case 'integer':
                return dbvalue || Number.NaN;
            case 'float':
                return dbvalue || Number.NaN;
            case 'date':
                return dbvalue ? new Date(dbvalue) : null;
        }
    }

    getPrefix(sqlobj) {
        let name = sqlobj.name ? sqlobj.name : sqlobj;
        name.db = name.db == '' ? 'public' : name.db;
        return `${this.database}.${name.db}.tables.${name.table}`;
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

    doWhere(where, row, namespace = false) {
        if (where === null) return true;

        var getVal = (obj) => { 
            if (obj.type === "column_ref") {
                let field = namespace ? obj.table + "." + obj.column : obj.column
                return row[field];
            }
            if (obj.type === "binary_expr") return this.doWhere(obj, row);
            return obj.value;
        }

        var replaceIfNotPrecededBy = (notPrecededBy, replacement) => {
            return function(match) {
                return match.slice(0, notPrecededBy.length) === notPrecededBy
                ? match
                : replacement;
            }
        }

        var like2RegExp = (like) => {
            var restring = like;
            restring = restring.replace(/([\.\*\?\$\^])/g, "\\$1");
            restring = restring.replace(/(?:\\)?%/g, replaceIfNotPrecededBy('\\', '.*?'));
            restring = restring.replace(/(?:\\)?_/g, replaceIfNotPrecededBy('\\', '.'));
            restring = restring.replace('\\%', '%');
            restring = restring.replace('\\_', '_');
            return new RegExp('^' + restring + '$');
        }

        switch (where.type) {
            case "binary_expr":
                switch(where.operator) {
                    case "=":
                        return getVal(where.left) == getVal(where.right);
                    case "!=":
                    case "<>":
                        return getVal(where.left) != getVal(where.right);
                    case "<":
                        return getVal(where.left) < getVal(where.right);
                    case "<=":
                        return getVal(where.left) <= getVal(where.right);
                    case ">":
                        return getVal(where.left) > getVal(where.right);
                    case ">=":
                        return getVal(where.left) >= getVal(where.right);
                    case "AND":
                        return getVal(where.left) && getVal(where.right);
                    case "OR":
                        return getVal(where.left) && getVal(where.right);
                    case "IS":
                        return getVal(where.left) === getVal(where.right)
                    case "LIKE":
                        return like2RegExp(getVal(where.right)).test(getVal(where.left)) === true;
                    case "NOT LIKE":
                        return like2RegExp(getVal(where.right)).test(getVal(where.left)) === false;
                    default:
                        return false;
                }
                break;
            default:
                return false;
        }
    }

    /**
     * Used to push a row into the data object. If the fields are limited
     * in the query, only places the requested fields.
     * 
     * @param {object} sqlObj 
     * @param {Array} data 
     * @param {object} row 
     * @returns 
     * @memberof Firebase
     */
    chooseFields(sqlObj, data, row, namespace = false) {
        if (sqlObj.columns === "*") {
            data.push(row);
            return;
        }

        let isAggregate = sqlObj.columns.some((col) => { return col.expr.type === 'aggr_func'; });

        if (isAggregate === true) {
            var groupby = () => {
                if (sqlObj.groupby == null) return 0;
                let result = data.findIndex(drow => {
                    return sqlObj.groupby.every(group => drow[group.column] == row[group.column]);
                });

                if (result == -1) {
                    data.push({});
                    return data.length - 1;
                }
                return result;
            }
            var index = groupby();

            for (let col of sqlObj.columns) {
                let name;
                switch(col.expr.type) {
                    case 'column_ref':
                        name = col.as || col.expr.column;
                        data[index][name] = row[col.expr.column];
                        break;
                    case 'aggr_func': 
                        name = col.as || col.expr.name.toUpperCase() + "(" + col.expr.args.expr.column + ")";
                        
                        switch(col.expr.name.toUpperCase()) {
                            case 'SUM':
                                if (data[index][name] === undefined) {
                                    data[index][name] = 0;
                                }
                                data[index][name] += row[col.expr.args.expr.column];
                                break;
                            case 'COUNT':
                                if (data[index][name] === undefined) {
                                    data[index][name] = 0;
                                }
                                data[index][name]++;
                                break;
                        }
                        break;
                }
            }
        } else {
            let result = {};
            for (let col of sqlObj.columns) {
                let name = col.as || (namespace ? col.expr.table + "." + col.expr.column : col.expr.column);
                result[name] = row[namespace ? col.expr.table + "." + col.expr.column : col.expr.column];
                if (result[name] === undefined) result[name] = null;
            }
            data.push(result);
        }
    }
    
    doCreate(sqlobj) {
        // create a new table definition
        let table_prefix = this.getPrefix(sqlobj);

        let columns = {};
        let n = 0;
        for (var col of sqlobj.columns) {
            var column = {
                name: col.name,
                index: n++
            }

            switch (col.type.type.toUpperCase()) {
                case 'CHAR':
                case 'CHARACTER':
                    column.type = 'string';
                    column.pad = ' ';
                    column.length = parseInt(col.type.args[0]);
                    break;
                case 'VARCHAR':
                    column.type = 'string';
                    column.length = parseInt(col.type.args[0]);
                    break;
                case 'BINARY':
                case 'VARBINARY':
                    column.type = 'binary';
                    column.length = parseInt(col.type.args[0]);
                    break;
                case 'BOOLEAN':
                    column.type = 'boolean';
                    break;
                case 'INTEGER':
                case 'SMALLINT':
                case 'BIGINT':                
                    column.type = 'integer';
                    break;
                case 'DECIMAL':
                case 'NUMERIC':
                case 'FLOAT':
                case 'REAL':
                case 'DOUBLE':
                    column.type = 'float';
                    break;
                case 'DATE':
                case 'TIME':
                case 'TIMESTAMP':
                    column.type = 'date';
                    break;
                case 'INTERVAL':
                case 'ARRAY':
                case 'MULTISET':
                case 'XML':
                    throw col.type.type.toUpperCase() + ' not yet supported';
                case 'TEXT':
                    column.type = 'string';
                    break;
            }

            columns[column.name] = column
        }
        storage.setItem(table_prefix + ".definition", JSON.stringify(columns));
    }

    doDelete(sqlobj) {
        if (sqlobj.from.length == 1) {
            let table = this.getPrefix(sqlobj.from[0]);
            let table_def = this.getTableDefinition(table);
            let rows = this.getMappedRows(table);
            let rowsWhere = rows.filter(row => !this.doWhere(sqlobj.where, row)); 
        

            let data = rowsWhere.map(row => {
                let fieldArray = Object.keys(table_def).map(k => row[k]);
                
                return fieldArray;
            });
            
            storage.setItem(table + ".rows", JSON.stringify(data));
        } else {
            throw "Delete from multiple tables is unsupported"
        }
    }
    
    doInsert(sqlobj) {
        let table_prefix = this.getPrefix(sqlobj);
        let table_definition = this.getTableDefinition(table_prefix);
        let table_columns = Object.values(table_definition).sort((a,b) => a.index - b.index);
        let table = this.getRows(table_prefix) || [];
        
        for (let row of sqlobj.values) {
            switch(row.type) {
                case 'expr_list':
                    let dbRow = table_columns.map(dbtype => this.getDefault(dbtype));
                    for (let col = 0; col < sqlobj.columns.length; col++) {
                        let coldef = table_definition[sqlobj.columns[col]];
                        let dbvalue = this.toValue(coldef, row.value[col].value);
                        let index = coldef.index;
                        dbRow[coldef.index] = dbvalue;
                    }
                    table.push(dbRow);
                    break;
                default:
                    console.warn('Unknown row type in insert:', row.type);
                    break;
            }
        }

        storage.setItem(table_prefix + ".rows", JSON.stringify(table));
    }

    doSingleSelect(sqlobj, rows, namespace = false) {
        let result = [];
        
        rows = rows.filter(row => this.doWhere(sqlobj.where, row, namespace));

        if (sqlobj.orderby) {
            rows.sort((a, b) => {
                for (let orderer of sqlobj.orderby) {
                    let column = namespace ? orderer.expr.table + "." + orderer.expr.column : orderer.expr.column;
                    if (orderer.expr.type !== 'column_ref') {
                        throw new Error("ORDER BY only supported for columns, aggregates are not supported");
                    }

                    if (a[column] > b[column]) {
                        return orderer.type == 'ASC' ? 1 : -1;
                    }
                    if (a[column] < b[column]) {
                        return orderer.type == 'ASC' ? -1 : 1;
                    }
                }
                return 0;
            });
        }

        rows.map(row => this.chooseFields(sqlobj, result, row, namespace));

        if (sqlobj.limit) {
            if (sqlobj.limit.length !== 2) {
                throw new Error("Invalid LIMIT expression: Use LIMIT [offset,] number");
            }
            let offs = parseInt(sqlobj.limit[0].value);
            let len = parseInt(sqlobj.limit[1].value);
            result = result.slice(offs, offs + len);
        }

        return result;
    }
    
    join(dest, src, query, includeAllDest, includeAllSrc, namespace = false) {
        var rows = [];

        let destRows = dest.map(row => {
            return { used: false, row: row };
        });

        let srcRows = src.map(row => {
            return { used: false, row: row };
        });

        for (let destRow of destRows) {

            for (let srcRow of srcRows) {
                var bigrow = {}
                for (var k in destRow.row) { bigrow[k] = destRow.row[k]; }
                for (var k in srcRow.row) { bigrow[k] = srcRow.row[k]; }
                if (this.doWhere(query, bigrow, namespace)) {
                    rows.push(bigrow);
                    destRow.used = true;
                    srcRow.used = true;
                }
            }
        }

        if (includeAllDest) {
            destRows.filter(row => row.used == false).map(row => rows.push(row.row));
        }
        if (includeAllSrc) {
            srcRows.filter(row => row.used == false).map(row => rows.push(row.row));
        }

        return rows;
    }

    /**
     * Joins:
     *      INNER: intersection of the two tables
     *      LEFT: first table plus second table or null
     *      RIGHT: second table plus first table or null
     *      FULL: both tables, null outside the intersection - apparently unsupported in sql parser
     */
    doSelect(sqlobj) {
        let tables = [];
        let namespace = sqlobj.from.length > 1;
        
        for (let n = 0; n < sqlobj.from.length; n++) {
            var table = this.getPrefix(sqlobj.from[n]);
            var from = sqlobj.from[n];
            let rows = this.getMappedRows(table, namespace ? sqlobj.from[n].table : false); 
            
            tables.push({ table: table, name: sqlobj.from[n].table, rows: rows, from: sqlobj.from[n] });
        }

        while (tables.length > 1) {
            // take the second table and merge it into the first according to the join rules
            switch(tables[1].from.join) {
                case 'INNER JOIN':
                    tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, false, false, namespace);
                    break;
                case 'LEFT JOIN':
                    tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, true, false, namespace);
                    break;
                case 'RIGHT JOIN':
                    tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, false, true, namespace);
                    break;
                case 'FULL JOIN':
                    tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, true, true, namespace);
                    break;
            }
            tables.splice(1,1);
        }

        // the join has been performed, now this is a big table treat it as such
        let result = this.doSingleSelect(sqlobj, tables[0].rows, namespace);

        return result;
    }
    
    doUpdate(sqlobj) {
        let table = this.getPrefix(sqlobj);
        let table_def = this.getTableDefinition(table);
        let rows = this.getMappedRows(table);
        let rowsWhere = rows.filter(row => this.doWhere(sqlobj.where, row)); 

        rowsWhere.map(row => {
            sqlobj.set.map(item => {
                if (item.value.type != table_def[item.column].type) throw `Invalid type mapping: ${item.value.type} is not ${table_def[item.column].type}`;
                row[item.column] = item.value.value;
            });
        });      

        let data = rows.map(row => {
            let fieldArray = Object.keys(table_def).map(k => row[k]);
            
            return fieldArray;
        });

        storage.setItem(table + ".rows", JSON.stringify(data));
    }

    parse(sql) {
        var sqlobj = parse(sql);

        
        switch(sqlobj.type) {
            case 'create_table': 
                return this.doCreate(sqlobj);
            case 'insert':
                return this.doInsert(sqlobj);
            case 'select':
                return this.doSelect(sqlobj);
            case 'update':
                return this.doUpdate(sqlobj);            
            case 'delete':
                return this.doDelete(sqlobj);
            default:
                console.log(JSON.stringify(sqlobj, null, 4));
                break;
        }

    }

    execute(sql) {
        return new Promise((resolve, reject) => {
            try {
                resolve(this.parse(sql));
            } catch (err) {
                reject(err);
            }
        });
    }

    query(sql) {
        return this.execute(sql);
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
