const fs = require('fs');

class LocalStorage {
    constructor() {
        try {
            var data = fs.readFileSync('./localStorage.json');
            this.data = JSON.parse(data);
        } catch (err) {
            this.data = {};
        }
    }

    __write() {
        try {
            fs.writeFileSync('./localStorage.json', JSON.stringify(this.data));
        } catch (e) {}
    }

    get length() {
        return Object.keys(this.data).length;
    }

    clear() {
        this.data = {};
        this.__write();
    }

    getItem(keyName) {
        if (keyName in this.data == false) {
            return null;
        }
        return this.data[keyName];
    }

    key(index) {
        if (index < length) {
            return Object.keys(this.data)[index];
        }
        return null;
    }

    removeItem(keyName) {
        if (keyName in this.data !== false) {
            delete this.data[keyName];
            this.__write();
        }
    }

    setItem(keyName, keyValue) {
        this.data[keyName] = keyValue.toString();
        this.__write();
    }
}

var store = new LocalStorage();
var proxy = new Proxy(store, {
    get: function(obj, prop) { 
        switch(prop) {
            case 'length':
                return store.length;
            case 'clear':
            case 'getItem':
            case 'key':
            case 'removeItem':
            case 'setItem':
                return store[prop].bind(store);
            default:
                return store.getItem(prop);
        }
    },
    set: function(obj, prop, value) { 
        switch(prop) {
            case 'length':
            case 'clear':
            case 'getItem':
            case 'key':
            case 'removeItem':
            case 'setItem':
                return false;
            default:
                store.setItem(prop, value);
                break;
        }
        return true;
    }
});

module.exports = proxy;