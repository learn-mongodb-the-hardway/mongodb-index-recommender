var db = db.getSiblingDB("quickstart");

// Drop collections
db.users.drop();
db.sights.drop();

// Enable profiling
db.setProfilingLevel(2, { slowms: 0 });

// Set up some data
db.users.insertOne({_id: 1, name: 'peter', address: { street: 'smiths road 16', city: 'london', country: 'uk' }});
db.sights.insertOne({_id: 1, user_id: 1, address: { street: 'smiths road 16', city: 'london', country: 'uk' }, name: "Peters house" });

// Create an index on name
db.users.createIndex({ name: 1 });

// Execute some query operation
db.users.findOne({name: 'peter'});
db.users.aggregate([
    { $match: { name: 'peter', city: "london" } },
    { $lookup: {
      from: "sights",
      localField: "_id",
      foreignField: "user_id",
      as: "sights"
    }}
])

// Disable profiling
db.setProfilingLevel(0, { slowms: 100 });

// Drop system profile collection
//db.system.profile.drop();