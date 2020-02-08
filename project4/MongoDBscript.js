//1.1
db.test.insertMany([
    {  
        "_id" : 20, 
        "name" : {
        	"first" : "Alex",
        	"last" : "Chen" 
        },
        "birth" : ISODate("1933-08-27T04:00:00Z"), 
        "death" : ISODate("1984-11-07T04:00:00Z"), 
        "contribs" : [
            "C++",
            "Simula"
        ],
        "awards" : [
          {
            "award" : "WPI Award", 
            "year" : 1977,
            "by" : "WPI"
          } 
        ]
    },
   
    {
    	"_id" : 30, 
    	"name" : {
			"first" : "David",
			"last" : "Mark"
		},
		"birth" : ISODate("1911-04-12T04:00:00Z"), 
		"death" : ISODate("2000-11-07T04:00:00Z"), 
		"contribs" : [
			"C++",
			"FP", 
			"Lisp",
		],
		"awards" : [
		  {
			"award" : "WPI Award",
			"year" : 1963,
			"by" : "WPI"
		  }, 
		  {
			"award" : "Turing Award", 
			"year" : 1966,
			"by" : "ACM"
		  }
		]
    }
]);

//1.2
db.test.find(
{
    $or:
    [
      {contribs:'FP'},
      {awards: {$size: 0}},
      {awards: {$size: 1}},
      {awards: {$size: 2}}
    ]
}
);

//1.3
db.test.update(
    {name:{"first":"Guido","last":"van Rossum"}},
    {$push:{contribs:"OOP"}}
);

//1.4
db.test.update(
    {name:{"first":"Alex","last":"Chen"}},
    {$set:{comments:["He taught in 3 universities","died from cancer","lived in CA"]}}
);

//1.5
var cursor = db.test.find(
    {
        name: {
            "first": "Alex",
            "last": "Chen"
        }
    }
)
var contributions = cursor.next().contribs;
for (var i = 0; i < contributions.length; i++) {
    var each_contrib = contributions[i];
    var cursor = db.test.aggregate(
        [
            {$unwind:"$contribs"},
            {$match:{'contribs':each_contrib}},
            {$group:{_id: "$contribs",people:{$push:"$name"}}}
        ]
    );
    cursor.forEach(printjson);
}

//1.6
db.test.distinct( "awards.by");

//1.7
db.test.update(
    {"awards.year": 2011},
    {$pull: {awards: {year: 2011}}},
    {multi: true}
);

//1.8
db.test.aggregate(
    [
        {$unwind:"$awards"},
        {$match:{'awards.year':2001}},
        {$group:{_id: "$name",count:{$sum:1}}},
        {$match:{count:{$gte: 2}}},
        {$project:{name:1}}
    ]
);

//1.9
var cursor = db.test.find().sort({_id:-1}).limit(1);
var max_id = cursor.next()._id;
db.test.find({_id:max_id});

//1.10
db.test.findOne({"awards.by": "ACM"});

//2.1
db.test.aggregate(
    [
        {$unwind:"$awards"},
        {$match:{awards:{$exists:true}}},
        {$group:{_id: "$awards.award",count:{$sum:1}}}
    ]
);

//2.2
db.test.aggregate(
    {$match:{birth:{$exists:true}}},
    {$group:{_id:{$year:"$birth"},
    idarray:{$addToSet:"$_id"}}
});

//2.3
var cursor_min = db.test.find().sort({_id:-1}).limit(1);
var cursor_max = db.test.find().sort({_id:1}).limit(1);
var min_id = cursor_min.next()._id;
var max_id = cursor_max.next()._id;
db.test.find(
    {
        $or:
        [{_id:max_id},
        {_id:min_id}]
    }
)