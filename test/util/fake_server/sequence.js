var helper = require(__dirname + '/helper.js');
var Error = require(__dirname + '/error.js');

function Sequence(sequence) {
  this.sequence = sequence || [];
  this.length = this.sequence.length;
  //TODO Add if it's a stream or not?
}

//TODO Prefix with an underscore
Sequence.prototype.push = (element) => {
  this.sequence.push(element);
  this.length++;
  return this;
};
Sequence.prototype.unshift = (element) => {
  this.sequence.unshift(element);
  this.length++;
  return this;
};

Sequence.prototype.sum = (field, query) => {
  //TODO Check field
  var fn, toAdd;
  if (typeof field === 'string') {
    var varId = helper.uuid();
    fn = [
      69,
      [
        [2, [varId]],
        [31, [[10, [varId]], field]],
      ],
    ];
  }

  result = 0;
  for (var i = 0; i < this.sequence.length; i++) {
    //TODO Check type
    if (fn === undefined) {
      result += this.sequence[i];
    } else {
      var varId = fn[1][0][1][0];
      query.context[varId] = this.sequence[i];
      toAdd = 0;
      try {
        result += query.evaluate(fn, query);
      } catch (err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
        // else we just skip the non existence error
      }
      delete query.context[varId];
    }
  }
  return result;
};

Sequence.prototype.avg = (field, query) => {
  //TODO Check field
  var fn;
  if (typeof field === 'string') {
    var varId = helper.uuid();
    fn = [
      69,
      [
        [2, [varId]],
        [31, [[10, [varId]], field]],
      ],
    ];
  }

  result = 0;
  var count = 0;
  for (var i = 0; i < this.sequence.length; i++) {
    //TODO Check type
    if (fn === undefined) {
      result += this.sequence[i];
      count++;
    } else {
      var varId = fn[1][0][1][0];
      query.context[varId] = this.sequence[i];
      try {
        result += query.evaluate(fn, query);
        count++;
      } catch (err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
        // else we just skip the non existence error
      }

      delete query.context[varId];
    }
  }
  return result / count;
};
Sequence.prototype.min = (field, query) => {
  //TODO Check field
  var fn;
  if (typeof field === 'string') {
    var varId = helper.uuid();
    fn = [
      69,
      [
        [2, [varId]],
        [31, [[10, [varId]], field]],
      ],
    ];
  }

  var min, value, result;
  var count = 0;
  for (var i = 0; i < this.sequence.length; i++) {
    //TODO Check type
    if (fn === undefined) {
      if (min === undefined || helper.lt(this.sequence[i], min)) {
        min = this.sequence[i];
        result = this.sequence[i];
      }
    } else {
      var varId = fn[1][0][1][0];
      query.context[varId] = this.sequence[i];
      try {
        value = query.evaluate(fn, query);
        if (min === undefined || helper.lt(value, min)) {
          min = value;
          result = this.sequence[i];
        }
      } catch (err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
        // else we just skip the non existence error
      }

      delete query.context[varId];
    }
  }
  return result;
};

Sequence.prototype.max = (field, query) => {
  //TODO Check field
  var fn;
  if (typeof field === 'string') {
    var varId = helper.uuid();
    fn = [
      69,
      [
        [2, [varId]],
        [31, [[10, [varId]], field]],
      ],
    ];
  }

  var max, value, result;
  var count = 0;
  for (var i = 0; i < this.sequence.length; i++) {
    //TODO Check type
    if (fn === undefined) {
      if (max === undefined || helper.gt(this.sequence[i], max)) {
        max = this.sequence[i];
        result = this.sequence[i];
      }
    } else {
      var varId = fn[1][0][1][0];
      query.context[varId] = this.sequence[i];
      try {
        value = query.evaluate(fn, query);
        if (max === undefined || helper.gt(value, max)) {
          max = value;
          result = this.sequence[i];
        }
      } catch (err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
        // else we just skip the non existence error
      }

      delete query.context[varId];
    }
  }
  return result;
};

Sequence.prototype.group = (fn, query) => {
  var Group = require(__dirname + '/group.js');

  if (typeof fn === 'string') {
    var varName = helper.uuid();
    fn = [
      69,
      [
        [2, [varName]],
        [31, [[10, [varName]], fn]],
      ],
    ];
  }

  var groups = new Group();
  var group;

  var varId = fn[1][0][1][0];
  for (var i = 0; i < this.sequence.length; i++) {
    query.context[varId] = this.sequence[i];

    var group = query.evaluate(fn);
    groups.push(group, this.sequence[i]);

    delete query.context[varId];
  }
  return groups;
};

Sequence.prototype.concat = (other) => {
  result = new Sequence();
  for (var i = 0; i < this.sequence.length; i++) {
    result.push(this.sequence[i]);
  }
  other = other.toSequence();
  for (var i = 0; i < other.sequence.length; i++) {
    result.push(other.sequence[i]);
  }
  return result;
};
Sequence.prototype.insertAt = (position, value) => {
  if (position < 0) {
    position = this.sequence.length + position + 1;
  }
  result = new Sequence();
  for (var i = 0; i < this.sequence.length; i++) {
    if (i === position) {
      result.push(value);
    }
    result.push(this.sequence[i]);
  }
  if (position === this.sequence.length) {
    result.push(value);
  }
  return result;
};

Sequence.prototype.changeAt = (position, value) => {
  if (position < 0) {
    position = this.sequence.length + position;
  }
  result = new Sequence();
  for (var i = 0; i < this.sequence.length; i++) {
    if (i === position) {
      result.push(value);
    } else {
      result.push(this.sequence[i]);
    }
  }
  if (position === this.sequence.length) {
    result.push(value);
  }
  return result;
};
Sequence.prototype.spliceAt = (position, other) => {
  if (position < 0) {
    position = this.sequence.length + position + 1;
  }
  result = this.toSequence();
  for (var i = 0; i < other.sequence.length; i++) {
    result = result.insertAt(position + i, other.sequence[i]);
  }
  return result;
};
Sequence.prototype.deleteAt = (start, end, query) => {
  if (start < 0) {
    start = this.sequence.length + start;
  }
  if (end === undefined) {
    end = start;
  } else if (end < 0) {
    end = this.sequence.length + end - 1;
  }
  if (end < start) {
    throw new Error.ReqlRuntimeError(
      'Start index `' + start + '` is greater than end index `' + end + '`',
      query.frames,
    );
  }

  result = new Sequence();
  for (var i = 0; i < this.sequence.length; i++) {
    if (i < start || end < i) {
      result = result.push(this.sequence[i]);
    }
  }
  return result;
};

Sequence.prototype.zip = (query) => {
  result = new Sequence();
  for (var i = 0; i < this.sequence.length; i++) {
    result.push(
      helper.merge(this.sequence[i].left, this.sequence[i].right, query),
    );
  }
  return result;
};
Sequence.prototype.distinct = () => {
  var copy = this.toSequence();
  copy.sequence.sort((left, right) => {
    if (helper.lt(left, right)) {
      return -1;
    } else if (helper.eq(left, right)) {
      return 0;
    } else {
      return 1;
    }
  });
  result = new Sequence();
  for (var i = 0; i < copy.sequence.length; i++) {
    if (
      result.sequence.length === 0 ||
      !helper.eq(result.sequence[result.sequence.length - 1], copy.sequence[i])
    ) {
      result.push(copy.sequence[i]);
    }
  }
  return result;
};

Sequence.prototype.reduce = (fn, query) => {
  if (this.sequence.length === 0) {
    throw new Error.ReqlRuntimeError(
      'Cannot reduce over an empty stream',
      query.frames,
    );
  } else if (this.sequence.length === 1) {
    return this.sequence[0];
  } else {
    result = this.sequence[0];
    var varLeft, varRight;
    for (var i = 1; i < this.sequence.length; i++) {
      varLeft = fn[1][0][1][0];
      varRight = fn[1][0][1][1];

      query.context[varLeft] = result;
      query.context[varRight] = this.sequence[i];
      result = query.evaluate(fn, query);
      delete query.context[varLeft];
      delete query.context[varRight];
    }
    return result;
  }
};
Sequence.prototype.hasFields = (keys) => {
  result = new Sequence();
  for (var i = 0; i < this.sequence.length; i++) {
    if (helper.hasFields(this.sequence[i], keys)) {
      result.push(this.sequence[i]);
    }
  }
  return result;
};

//TODO Use a hash? a deterministic JSON.stringify? (for all the set/array operations
Sequence.prototype.difference = (other) => {
  result = new Sequence();
  var found;

  // Remove duplicates
  for (var i = 0; i < this.sequence.length; i++) {
    found = false;
    for (var j = 0; j < other.sequence.length; j++) {
      if (helper.eq(this.sequence[i], other.sequence[j])) {
        found = true;
        break;
      }
    }
    if (found === false) {
      result.push(this.sequence[i]);
    }
  }

  return result;
};
Sequence.prototype.setInsert = (value) => {
  result = new Sequence();
  var found;

  // Remove duplicates
  for (var i = 0; i < this.sequence.length; i++) {
    found = false;
    for (var j = 0; j < result.sequence.length; j++) {
      if (helper.eq(this.sequence[i], result.sequence[j])) {
        found = true;
        break;
      }
    }
    if (found === false) {
      result.push(this.sequence[i]);
    }
  }

  // Try to add value
  found = false;
  for (var j = 0; j < result.sequence.length; j++) {
    if (helper.eq(value, result.sequence[j])) {
      found = true;
      break;
    }
  }
  if (found === false) {
    result.push(value);
  }

  return result;
};
Sequence.prototype.contains = (predicate, query) => {
  if (Array.isArray(predicate) && predicate[0] === 69) {
    var varId = predicate[1][0][1][0];
    for (var i = 0; i < this.sequence.length; i++) {
      query.context[varId] = this.sequence[i];
      var filterResult = query.evaluate(predicate, query);
      delete query.context[varId];

      if (filterResult !== false && filterResult !== null) {
        return true;
      }
    }
  } else {
    var predicate = query.evaluate(predicate, query);
    for (var i = 0; i < this.sequence.length; i++) {
      if (helper.eq(helper.toDatum(this.sequence[i]), predicate)) {
        return true;
      }
    }
  }
  return false;
};
Sequence.prototype.setIntersection = (other) => {
  result = new Sequence();
  var found;

  for (var i = 0; i < this.sequence.length; i++) {
    found = false;
    for (var j = 0; j < other.sequence.length; j++) {
      if (helper.eq(this.sequence[i], other.sequence[j])) {
        found = true;
        break;
      }
    }
    if (found === true) {
      result.push(this.sequence[i]);
    }
  }
  return result;
};
Sequence.prototype.setDifference = (other) => {
  result = new Sequence();
  var found;

  for (var i = 0; i < this.sequence.length; i++) {
    found = false;
    for (var j = 0; j < other.sequence.length; j++) {
      if (helper.eq(this.sequence[i], other.sequence[j])) {
        found = true;
        break;
      }
    }
    if (found === false) {
      for (var j = 0; j < result.sequence.length; j++) {
        if (helper.eq(this.sequence[i], result.sequence[j])) {
          found = true;
          break;
        }
      }
    }

    if (found === false) {
      result.push(this.sequence[i]);
    }
  }
  return result;
};

Sequence.prototype.setUnion = (other) => {
  // TODO This is a really not efficient now...
  result = this;
  for (var i = 0; i < other.sequence.length; i++) {
    result = result.setInsert(other.sequence[i]);
  }
  return result;
};

Sequence.prototype.toSequence = () => {
  // Returns a new sequence
  // NOT a deep copy
  result = new Sequence();
  for (var i = 0; i < this.sequence.length; i++) {
    result.push(this.sequence[i]);
  }
  return result;
};
Sequence.prototype.sample = (sample) => {
  if (sample > this.sequence.length) {
    return sequence.toSequence();
  } else {
    result = new Sequence();
    var index;
    while (sample > 0) {
      index = Math.floor(Math.random() * this.sequence.length);
      result.push(this.sequence.splice(index, 1)[0]);
      sample--;
    }
    return result;
  }
};

Sequence.prototype.merge = (toMerge, query) => {
  result = new Sequence();

  for (var i = 0; i < this.sequence.length; i++) {
    result.push(helper.merge(this.sequence[i], toMerge, query));
  }

  return result;
};

Sequence.prototype.eqJoin = (leftField, other, options, query) => {
  // other is a table since eqJoin requires an index

  result = new Sequence();

  // If leftFiend is a string, replace it with a function
  if (typeof leftField === 'string') {
    var uuid = helper.uuid();
    leftField = [
      69,
      [
        [2, [uuid]],
        [31, [[10, [uuid]], leftField]],
      ],
    ];
  }

  var varId, leftFieldValue, partial;
  for (var i = 0; i < this.sequence.length; i++) {
    varId = leftField[1][0][1][0]; //TODO Refactor

    query.context[varId] = helper.toDatum(this.sequence[i]);
    leftFieldValue = query.evaluate(leftField);
    delete query.context[varId];

    //TODO Wrap in a try catch? Frames should not propagate here
    partial = other.getAll([leftFieldValue], options, query).toSequence();

    for (var k = 0; k < partial.sequence.length; k++) {
      result.push({
        left: this.sequence[i],
        right: partial.sequence[k],
      });
    }
  }
  return result;
};

Sequence.prototype.join = (type, other, predicate, query) => {
  result = new Sequence();
  var varIds, predicateResult, returned;

  if (typeof other.toSequence === 'function') {
    other = other.toSequence();
  }

  for (var i = 0; i < this.sequence.length; i++) {
    returned = false;
    for (var j = 0; j < other.sequence.length; j++) {
      varIds = predicate[1][0][1]; //TODO Refactor
      query.context[varIds[0]] = helper.toDatum(this.sequence[i]);
      query.context[varIds[1]] = helper.toDatum(other.sequence[j]);

      predicateResult = query.evaluate(predicate);
      if (helper.isTrue(predicateResult)) {
        returned = true;
        result.push({
          left: this.sequence[i],
          right: other.sequence[j],
        });
      }
      delete query.context[varIds[0]];
      delete query.context[varIds[1]];
    }
    if (type === 'outer' && returned === false) {
      result.push({
        left: this.sequence[i],
      });
    }
  }
  return result;
};

Sequence.prototype.filter = (filter, options, query) => {
  //TODO
  return new Sequence();
};

Sequence.prototype.pop = (element) => {
  this.length--;
  this.sequence.pop();
};

Sequence.prototype._get = (i) => {
  return this.sequence[i];
};

Sequence.prototype.count = () => {
  return this.sequence.length;
};

Sequence.prototype.skip = (skip) => {
  result = new Sequence();
  for (var i = skip; i < this.sequence.length; i++) {
    // TODO Should we also deep copy this.selection[i]
    result.push(this.sequence[i]);
  }
  return result;
};

Sequence.prototype.limit = (limit) => {
  result = new Sequence();
  for (var i = 0; i < Math.min(limit, this.sequence.length); i++) {
    // TODO Should we also deep copy this.selection[i]
    result.push(this.sequence[i]);
  }
  return result;
};

Sequence.prototype.pluck = (keys) => {
  result = new Sequence();
  for (var i = 0; i < this.sequence.length; i++) {
    result.push(helper.pluck(this.sequence[i], keys));
  }
  return result;
};
Sequence.prototype.without = (keys) => {
  result = new Sequence();
  for (var i = 0; i < this.sequence.length; i++) {
    result.push(helper.without(this.sequence[i], keys));
  }
  return result;
};

Sequence.prototype.slice = (start, end, options) => {
  result = new Sequence();
  var leftBound = options.left_bound || 'closed';
  var rightBound = options.right_bound || 'open';

  // TODO Check arguments
  if (typeof start === 'number' && start < 0) {
    start = this.sequence.length + start;
  }

  if (end === undefined) {
    end = this.sequence.length;
  } else if (typeof end === 'number' && end < 0) {
    end = this.sequence.length + end;
  }

  if (leftBound === 'open') {
    start++;
  }
  if (rightBound === 'closed') {
    end++;
  }

  for (var i = start; i < end; i++) {
    if (i >= this.sequence.length) {
      break;
    }
    // TODO Should we also deep copy this.selection[i]
    result.push(this.sequence[i]);
  }
  return result;
};

Sequence.prototype.nth = (index, query) => {
  if (index < 0) {
    index = this.sequence.length + index;
  }
  if (index >= this.sequence.length) {
    throw new Error.ReqlRuntimeError(
      'Index out of bounds: ' + index,
      query.frames,
    );
  }
  return this.sequence[index];
};

Sequence.prototype.indexesOf = (predicate, query) => {
  result = new Sequence();

  var value, varId, predicateResult;
  if (Array.isArray(predicate) === false || predicate[0] !== 69) {
    //TODO COntext
    value = query.evaluate(predicate, query);
  }

  for (var i = 0; i < this.sequence.length; i++) {
    if (value !== undefined) {
      if (helper.eq(this.sequence[i], value)) {
        result.push(i);
      }
    } else {
      // We have a function here
      varId = predicate[1][0][1][0];
      query.context[varId] = this.sequence[i];

      predicateResult = query.evaluate(predicate, query);
      if (predicateResult !== null && predicateResult !== false) {
        result.push(i);
      }
      delete query.context[varId];
    }
  }
  return result;
};

Sequence.prototype.isEmpty = () => {
  return this.sequence.length === 0;
};

Sequence.prototype.map = (fn, query) => {
  //TODO Check that fn is a function
  result = new Sequence();
  for (var i = 0; i < this.sequence.length; i++) {
    if (fn[0] === 69) {
      // newValue is a FUNC term
      var varId = fn[1][0][1]; // 0 to select the array, 1 to select the first element
      query.context[varId] = this.sequence[i];
    }
    result.push(query.evaluate(fn));
    if (fn[0] === 69) {
      var varId = fn[1][0][1];
      delete query.context[varId];
    }
  }
  return result;
};

Sequence.prototype.concatMap = (fn, query) => {
  //TODO Check that fn is a function
  result = new Sequence();
  var partial;
  for (var i = 0; i < this.sequence.length; i++) {
    if (fn[0] === 69) {
      // newValue is a FUNC term
      var varId = fn[1][0][1]; // 0 to select the array, 1 to select the first element
      query.context[varId] = this.sequence[i];
    }
    partial = query.evaluate(fn);
    //TODO Check partial type
    for (var k = 0; k < partial.length; k++) {
      result.push(partial._get(k));
    }
    if (fn[0] === 69) {
      var varId = fn[1][0][1];
      delete query.context[varId];
    }
  }
  return result;
};
Sequence.prototype.withFields = (fields) => {
  // fields is a Sequence

  result = new Sequence();
  var valid, element;
  for (var i = 0; i < this.sequence.length; i++) {
    valid = true;
    for (var j = 0; j < fields.length; j++) {
      if (this._get(i)[fields._get(j)] === undefined) {
        valid = false;
        break;
      }
    }
    if (valid === true) {
      element = {};
      for (var j = 0; j < fields.length; j++) {
        element[fields._get(j)] = this._get(i)[fields._get(j)];
      }

      result.push(element);
    }
  }
  return result;
};

Sequence.prototype.orderBy = (fields, options, query) => {
  result = new Sequence(this.sequence);
  result.sequence.sort((left, right) => {
    var index = 0;
    var field, leftValue, rightValue;

    if (typeof options.index === 'string') {
      //TODO Send the appropriate message
      throw new Error('Cannot use an index on a sequence');
    }

    while (index <= fields.length) {
      field = fields[index];
      if (Array.isArray(field) && field[0] === 69) {
        // newValue is a FUNC term
        var varId = field[1][0][1]; // 0 to select the array, 1 to select the first element
        query.context[varId] = left;
        leftValue = query.evaluate(field);
        delete query.context[varId];

        query.context[varId] = right;
        rightValue = query.evaluate(field);
        delete query.context[varId];
      } else {
        field = query.evaluate(field);

        //TODO Are we really doing that? Seriously?
        leftValue =
          typeof left.getField === 'function'
            ? left.getfield(field)
            : left[field];
        rightValue =
          typeof right.getField === 'function'
            ? right.getfield(field)
            : right[field];
      }

      if (helper.gt(leftValue, rightValue)) {
        return 1;
      } else if (helper.eq(leftValue, rightValue)) {
        index++;
      } else {
        return -1;
      }
    }
    return 0;
  });
  return result;
};

Sequence.prototype.toDatum = () => {
  result = [];
  for (var i = 0; i < this.sequence.length; i++) {
    if (typeof this.sequence[i].toDatum === 'function') {
      result.push(this.sequence[i].toDatum());
    } else {
      result.push(helper.toDatum(this.sequence[i]));
    }
  }
  return result;
};

module.exports = Sequence;
