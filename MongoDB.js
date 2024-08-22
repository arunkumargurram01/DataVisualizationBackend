require('dotenv').config();
const mongoose = require('mongoose');



const connectDB =() => {
    try {
         mongoose.connect(process.env.MongoDB_URI, {
            useNewUrlParser: true,
            useUnifiedTopology: true
        });
        console.log(`MongoDB Connected`);

       const db = mongoose.connection.db;

    } catch (err) {
        console.error(`MongoDB connection error: ${err}`);
    }
};


// Define the schema (we can use an empty schema if we only want to use Mongoose for querying)
const orderSchema = new mongoose.Schema({}, { strict: false });
const Order = mongoose.model('Order', orderSchema, 'shopifyOrders');




const calculateGrowthRate = (current, previous) => {
    if (previous === 0) return 0;  // Avoid division by zero
    return ((current - previous) / previous * 100).toFixed(2);
};




const salesOverTime = async () => {
    try {

        // Daily Aggregation
        const dailySales = await Order.aggregate([
            {
                $addFields: {
                    created_at: { $dateFromString: { dateString: "$created_at" } },
                    total_price: { $toDouble: "$total_price_set.shop_money.amount" }
                }
            },
            {
                $group: {
                    _id: { $dateToString: { format: "%Y-%m-%d", date: "$created_at" } },
                    totalSales: { $sum: "$total_price" }
                }
            },
            { $sort: { _id: 1 } }
        ]);

         // Monthly Aggregation
         const monthlySales = await Order.aggregate([
            {
                $addFields: {
                    created_at: { $dateFromString: { dateString: "$created_at" } },
                    total_price: { $toDouble: "$total_price_set.shop_money.amount" }
                }
            },
            {
                $group: {
                    _id: { $dateToString: { format: "%Y-%m", date: "$created_at" } },
                    totalSales: { $sum: "$total_price" }
                }
            },
            { $sort: { _id: 1 } }
        ]);

        // Quarterly Aggregation
        const quarterlySales = await Order.aggregate([
            {
                $addFields: {
                    created_at: { $dateFromString: { dateString: "$created_at" } },
                    total_price: { $toDouble: "$total_price_set.shop_money.amount" }
                }
            },
            {
                $group: {
                    _id: {
                        $concat: [
                            { $toString: { $year: "$created_at" } },
                            "-Q",
                            { $toString: { $ceil: { $divide: [{ $month: "$created_at" }, 3] } } }
                        ]
                    },
                    totalSales: { $sum: "$total_price" }
                }
            },
            {
                $project: {
                    _id: 1, // The "_id" now contains "year-quarter"
                    totalSales: 1
                }
            },
            { $sort: { _id: 1 } }
        ]);

        // Yearly Aggregation
        const yearlySales = await Order.aggregate([
            {
                $addFields: {
                    created_at: { $dateFromString: { dateString: "$created_at" } },
                    total_price: { $toDouble: "$total_price_set.shop_money.amount" }
                }
            },
            {
                $group: {
                    _id: { $year: "$created_at" },
                    totalSales: { $sum: "$total_price" }
                }
            },
            { $sort: { _id: 1 } }
        ]);
        //Calculating salre growth rate

        // Calculate Growth Rates
        const calculateGrowth = (sales) => {
            return sales.reduce((acc, curr, index, array) => {
                if (index === 0) {
                    acc.push({ ...curr, growthRate: null });  
                } else {
                    const previous = array[index - 1].totalSales;
                    const growthRate = calculateGrowthRate(curr.totalSales, previous);
                    acc.push({ ...curr, growthRate });
                }
                return acc;
            }, []);
        };

        const dailysalesgrowth= calculateGrowth(dailySales);
        const monthlysalesgrowth= calculateGrowth(monthlySales);
        const quarterlysalesgrowth= calculateGrowth(quarterlySales);
        const yearlysalesgrowth= calculateGrowth(yearlySales);


        const salesData  = {
            "daily" : dailySales,
            "monthly" : monthlySales,
            "quarterly" : quarterlySales,
            "yearly" : yearlySales,
            "gdaily" : dailysalesgrowth,
            "gmonthly" : monthlysalesgrowth,
            "gquarterly" : quarterlysalesgrowth,
            "gyearly" : yearlysalesgrowth,
        }    
        //console.log(salesData)
        return salesData;

    }catch(err){
        console.log(`Error from GetAggrigation : ${err}`)
    }
}

//CODE for New Customers Added Over Time:
const CustomerSchema = new mongoose.Schema({}, { strict: false });
const CustomerModel = mongoose.model('Customer', CustomerSchema, 'shopifyCustomers');

const newCustomersOverTime = async () => {
    try {
        // Daily Aggregation
        const dailyCustomers = await CustomerModel.aggregate([
            {
                $addFields: {
                    created_at: { $dateFromString: { dateString: "$created_at" } }
                }
            },
            {
                $group: {
                    _id: { $dateToString: { format: "%Y-%m-%d", date: "$created_at" } },
                    totalCustomers: { $sum: 1 }
                }
            },
            { $sort: { _id: 1 } }
        ]);

        // Monthly Aggregation
        const monthlyCustomers = await CustomerModel.aggregate([
            {
                $addFields: {
                    created_at: { $dateFromString: { dateString: "$created_at" } }
                }
            },
            {
                $group: {
                    _id: { $dateToString: { format: "%Y-%m", date: "$created_at" } },
                    totalCustomers: { $sum: 1 }
                }
            },
            { $sort: { _id: 1 } }
        ]);

        // Quarterly Aggregation
        const quarterlyCustomers = await CustomerModel.aggregate([
            {
                $addFields: {
                    created_at: { $dateFromString: { dateString: "$created_at" } }
                }
            },
            {
                $group: {
                    _id: {
                        $concat: [
                            { $toString: { $year: "$created_at" } },
                            "-Q",
                            { $toString: { $ceil: { $divide: [{ $month: "$created_at" }, 3] } } }
                        ]
                    },
                    totalCustomers: { $sum: 1 }
                }
            },
            {
                $project: {
                    _id: 1,
                    totalCustomers: 1
                }
            },
            { $sort: { _id: 1 } }
        ]);

        // Yearly Aggregation
        const yearlyCustomers = await CustomerModel.aggregate([
            {
                $addFields: {
                    created_at: { $dateFromString: { dateString: "$created_at" } }
                }
            },
            {
                $group: {
                    _id: { $year: "$created_at" },
                    totalCustomers: { $sum: 1 }
                }
            },
            { $sort: { _id: 1 } }
        ]);

        const customersData  = {
            "daily" : dailyCustomers,
            "monthly" : monthlyCustomers,
            "quarterly" : quarterlyCustomers,
            "yearly" : yearlyCustomers,
        }   

        return customersData;

    } catch (error) {
        console.error('Error in fetching new customers:', error.message);
        throw error;
    }
};


//CODE for Number of Repeat Customers
const repeatCustomers = async () => {
    try {
        const dailyRepeatCustomers = await Order.aggregate([
            {
                $group: {
                    _id: {
                        customer_id: "$customer.id",
                        date: { $dateToString: { format: "%Y-%m-%d", date: { $toDate: "$created_at" } } }
                    },
                    orderCount: { $sum: 1 }
                }
            },
            {
                $match: {
                    orderCount: { $gt: 1 }
                }
            },
            {
                $group: {
                    _id: "$_id.date",
                    repeatCustomerCount: { $sum: 1 }
                }
            },
            {
                $sort: { _id: 1 }
            }
        ]);


        //monthly repeated cutomers
        const monthlyRepeatCustomers = await Order.aggregate([
            {
                $group: {
                    _id: {
                        customer_id: "$customer.id",
                        date: { $dateToString: { format: "%Y-%m", date: { $toDate: "$created_at" } } }
                    },
                    orderCount: { $sum: 1 }
                }
            },
            {
                $match: {
                    orderCount: { $gt: 1 }
                }
            },
            {
                $group: {
                    _id: "$_id.date",
                    repeatCustomerCount: { $sum: 1 }
                }
            },
            {
                $sort: { _id: 1 }
            }
        ]);


        const quarterlyRepeatCustomers = await Order.aggregate([
            {
                $group: {
                    _id: {
                        customer_id: "$customer.id",
                        quarter: {
                            $concat: [
                                { $substr: [{ $toString: { $year: { $toDate: "$created_at" } } }, 0, 4] },
                                "-Q",
                                { $toString: { $ceil: { $divide: [{ $month: { $toDate: "$created_at" } }, 3] } } }
                            ]
                        }
                    },
                    orderCount: { $sum: 1 }
                }
            },
            {
                $match: {
                    orderCount: { $gt: 1 }
                }
            },
            {
                $group: {
                    _id: "$_id.quarter",
                    repeatCustomerCount: { $sum: 1 }
                }
            },
            {
                $sort: { _id: 1 }
            }
        ]);


        const yearlyRepeatCustomers = await Order.aggregate([
            {
                $group: {
                    _id: {
                        customer_id: "$customer.id",
                        year: { $dateToString: { format: "%Y", date: { $toDate: "$created_at" } } }
                    },
                    orderCount: { $sum: 1 }
                }
            },
            {
                $match: {
                    orderCount: { $gt: 1 }
                }
            },
            {
                $group: {
                    _id: "$_id.year",
                    repeatCustomerCount: { $sum: 1 }
                }
            },
            {
                $sort: { _id: 1 }
            }
        ]);

        const repeatCustomersData  = {
            "daily" : dailyRepeatCustomers,
            "monthly" : monthlyRepeatCustomers,
            "quarterly" : quarterlyRepeatCustomers,
            "yearly" : yearlyRepeatCustomers,
        }

        return repeatCustomersData;


    } catch (error) {
        console.error(error.message);
    }
};



//Code for Geographical Distribution of Customers
const CustomerLocationModel = mongoose.model('Customer', CustomerSchema, 'shopifyCustomers');

const geographicalLocations = async () => {
    try {
        const distribution = await CustomerLocationModel.aggregate([
            {
                $group: {
                    _id: "$default_address.city",
                    count: { $sum: 1 }
                }
            },
            {
                $sort: { count: -1 }
            }
        ]);
        return distribution;
        
    } catch (error) {
        console.error(`Error From getGeographicalLocations func : ${error.message}`);
        throw error;
    }
};

// Function to calculate CLV by cohorts
const customerCohorts = async () => {
    try {
        const clvByCohort = await CustomerModel.aggregate([
            {
                $lookup: {
                    from: "shopifyOrders",
                    localField: "email", //'email' is the link between customers and orders
                    foreignField: "email",
                    as: "orders"
                }
            },
            {
                $unwind: "$orders"
            },
            {
                $addFields: {
                    "orders.total_price": { $toDouble: "$orders.total_price_set.shop_money.amount" }
                }
            },
            {
                $group: {
                    _id: {
                        cohort: { $dateToString: { format: "%Y-%m", date: { $dateFromString: { dateString: "$created_at" } } } }, // cohort based on first purchase month
                        customer: "$_id"
                    },
                    totalSpent: { $sum: "$orders.total_price" }
                }
            },
            {
                $group: {
                    _id: "$_id.cohort",
                    cohortLifetimeValue: { $sum: "$totalSpent" },
                    customerCount: { $sum: 1 }
                }
            },
            {
                $sort: { _id: 1 } 
            }
        ]);

        return clvByCohort;
    } catch (error) {
        console.error(error.message);
        throw error;
    }
};


//Exporting all 
module.exports = {
    connectDB,
    salesOverTime,
    newCustomersOverTime,
    repeatCustomers,
    geographicalLocations,
    customerCohorts,

} 


// Function to get specific fields from orders
// const getOrders = async () => {
//     try {
//         const orders = await Order.find({}, {
//             created_at: 1,
//             'total_price_set.shop_money.amount': 1,
//             _id: 0  // Exclude _id if you don't want it
//         }).limit(10);  // Limit for debugging
//         console.log(`Orders = ${JSON.stringify(orders, null, 2)}`);
//         return orders;
//     } catch (error) {
//         console.error(error.message);
//         throw error;
//     }
// };