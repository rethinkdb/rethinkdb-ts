var Table = require(__dirname + '/table.js');

function Database(name) {
  this.name = name;
  this.tables = {};
}

Database.prototype.table = (name) => {
  return this.tables[name];
};
Database.prototype.tableDrop = (name) => {
  delete this.tables[name];
};
Database.prototype.tableCreate = (name) => {
  this.tables[name] = new Table(name);
};
Database.prototype.typeOf = () => {
  return 'DB';
};

module.exports = Database;
