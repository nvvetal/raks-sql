'use strict';
var mysql = require('mysql');

var connection;
var self;

function Wap3LibSQL(options) {
    self = this;
    this.showLog = options.showLog !== 'undefined' ? options.showLog : false;
    this.db = options.db ? options.db : {
        host: 'localhost',
        port: 3306,
        user: '',
        password: '',
        database: '',
        charset: 'utf8_general_ci',
    };
    this.multipleStatements =  options.multipleStatements !== 'undefined' ? options.multipleStatements : false;
    return connection;
}

Wap3LibSQL.prototype.showLog = false;

Wap3LibSQL.prototype.connect = function (callback) {

    connection = mysql.createConnection({
        host: self.db.host,
        user: self.db.user,
        password: self.db.password,
        database: self.db.database,
        multipleStatements: self.multipleStatements,
        charset: self.db.charset,
    });

    connection.connect(function (err) {              // The server is either down
        if (err) {                                     // or restarting (takes a while sometimes).
            console.log('SQL start connect error', err);
            setTimeout(self.handleDisconnect, 2000); // We introduce a delay before attempting to reconnect,
        }                                     // to avoid a hot loop, and to allow our node script to
        if(self.showLog) console.log('SQL connected');
        return callback && callback(err);
    });                                     // process asynchronous requests in the meantime.
    // If you're also serving http, display a 503 error.
    connection.on('error', function (err) {
        console.log('SQL connection error', err);
        if (err.fatal || err.code === 'PROTOCOL_CONNECTION_LOST') { // Connection to the MySQL server is usually
            self.handleDisconnect();                         // lost due to either server restart, or a
        }
    });
};

Wap3LibSQL.prototype.handleDisconnect = function () {
    console.log('SQL handling disconnect');
    self.connect();
};

Wap3LibSQL.prototype.connectionEnd = function(callback) {
    connection.end(function(err){
        callback && callback(err);
    });
};

/**
 *
 * @param query
 * @param params
 * @param callback
 * @param {Object|undefined} options
 */
Wap3LibSQL.prototype.query = function(query, params, callback, options){
    if(self.showLog) console.log('SQL query', query, params);
    return connection.query(query, params, function(err, rows){
        if(err) {
            console.log('SQL query result error', err);
            return callback && callback(err);
        }
        if(self.showLog) console.log('SQL query result', rows);
        return callback && callback(err, rows);
    });
};

/**
 *
 * @param query
 * @param params
 * @param callback
 * @param options
 */
Wap3LibSQL.prototype.getOne = function(query, params, callback, options){
    if(self.showLog) console.log('SQL GET ONE', query, params);
    return this.query(query, params, function(err, rows){
        if(err) {
            console.log('GET ONE ERR', err);
            return callback && callback(err);
        }
        if(self.showLog) console.log('GET ONE RESULT', rows[0]);
        return callback && callback(err, rows.length > 0 && rows[0]);
    });
};

/**
 *
 * @param query
 * @param params
 * @param callback
 * @param options
 */
Wap3LibSQL.prototype.insertQuery = function(query, params, callback, options){
    if(self.showLog) console.log('SQL INSERT', query, params);
    return this.query(query, params, function(err, result){
        if(err) {
            return callback && callback(err);
        }
        return callback && callback(err, result.insertId);
    });
};

Wap3LibSQL.prototype.escape = function(param){
    connection.escape(param);
};


/**
 *
 * @param table
 * @param hash
 * @param callback
 * @param options
 */
Wap3LibSQL.prototype.insertQueryAll = function(table, hash, callback, options){
    let i, j, k;
    if(!Array.isArray(hash)){
        return callback && callback('values must be array of objects');
    }
        let iKeys = {}, iOrder = [], tmp = [], tmp2;
        for (i = 0; i < hash.length; i++) {
            for (k in hash[i]) {
                if (!iKeys[k]) {
                    iKeys[k] = 1;
                    iOrder.push(k);
                    tmp.push('`' + k + '`');
                }
            }
        }
        let sqlQuery = "INSERT INTO `" + table + "` (" + tmp.join(', ') + ") VALUES ";
        tmp = [];
        for (i = 0; i < hash.length; i++) {
            tmp2 = [];
            for (j = 0; j < iOrder.length; j++) {
                tmp2.push(hash[i][iOrder[j]])
            }
            tmp.push(tmp2)
        }
        sqlQuery += connection.escape(tmp);
        if(self.showLog) console.log(sqlQuery);
        return self.query(sqlQuery, [], function (err, result) {
            if(err) {
                return callback && callback(err);
            }
            return callback && callback(err, result.insertId);
        });
};

Wap3LibSQL.prototype.beginTransaction = function(options, callback) {
  connection.beginTransaction(options, function(err) {
      if(err && self.showLog) console.log('[ERR beginTransaction]', err);
      callback && callback(err);
  });
};

Wap3LibSQL.prototype.rollback = function(options, callback) {
  connection.rollback(options, function(err) {
      if(err && self.showLog) console.log('[ERR rollback]', err);
      callback && callback(err);
  });
};

Wap3LibSQL.prototype.commit = function(options, callback) {
  connection.commit(options, function(err) {
      if(err && self.showLog) console.log('[ERR commit]', err);
      callback && callback(err);
  });
};


module.exports = Wap3LibSQL;