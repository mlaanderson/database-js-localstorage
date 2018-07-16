# database-js-firebase
[![Build Status](https://travis-ci.org/mlaanderson/database-js-localstorage.svg?branch=master)](https://travis-ci.org/mlaanderson/database-js-localstorage)

Database-js interface for Web Browser Local Storage

## About
Database-js-localstorage is a [database-js](https://github.com/mlaanderson/database-js) driver which uses the web browser local storage as the backend. It supports
table creation, selects, inserts, deletes, and updates. Selects can use inner, left and right joins. Outer joins are not yet supported by the SQL parser. It supports schemas, 
the default schema is called "public".

Database-js-localstorage includes a very basic localstorage implementation for NodeJS. It saves to and reads from a file called "localstorage.json";


## Install

```shell
npm install database-js database-js-localstorage
```

## Usage:
```javascript
var Connection = require('database-js').Connection;

(async () => {
    let connection, statement, rows;
    connection = new Connection('localstorage:///[database-name]');
    
    try {
        statement = await connection.prepareStatement("SELECT * FROM users WHERE username = ?");
        rows = await statement.query('dduck');
        console.log(rows);
    } catch (error) {
        console.log(error);
    } finally {
        await connection.close();
    }
})();
```

In the browser, you have to load the database-js-localstorage driver yourself and pass it to the Connection class:
```javascript
var Connection = require('database-js').Connection;
var Driver = require('database-js-localstorage');

(async () => {
    let connection, statement, rows;
    connection = new Connection('localstorage:///[database-name]', Driver);
    
    try {
        statement = await connection.prepareStatement("SELECT * FROM users WHERE username = ?");
        rows = await statement.query('dduck');
        console.log(rows);
    } catch (error) {
        console.log(error);
    } finally {
        await connection.close();
    }
})();
```

## License

[MIT](https://github.com/mlaanderson/database-js/blob/master/LICENSE) (c) [mlaanderson](https://github.com/mlaanderson)