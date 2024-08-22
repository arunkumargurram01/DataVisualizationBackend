const express =  require('express')
const cors = require('cors');
const MongoDB = require('./MongoDB')
const bodyParser = require('body-parser')
require('dotenv').config();

const app = express();

const allowedOrigin = process.env.FRONTEND_URL || '*'; // Fallback to wildcard for testing
const corsOptions = {
    origin: allowedOrigin,
    credentials: true,
    optionsSuccessStatus: 200
};

app.use(cors());
app.use(bodyParser.json());
app.use(express.json());


app.get(('/'),async (req,res) =>{
    //const data = await MongoDB.getAggregatedSales()
    res.send("Server is Online")
    //res.json({"key": data.yearly})
})

app.get(('/salesovertime'),async(req, res) => {
    const data = await MongoDB.salesOverTime()
    res.send(data)
})

app.get(('/salesgrowthovertime'), async(req, res) => {
    const data = await MongoDB.salesOverTime()
    res.send(data)
})
app.get(('/newcustomersovertime'), async(req, res) => {
    const data = await MongoDB.newCustomersOverTime()
    res.send(data)
})
app.get(('/repetedcustomers'), async(req, res) => {
    const data = await MongoDB.repeatCustomers()
    res.send(data)
})
app.get(('/customerslocations'), async(req, res) => {
    const data = await MongoDB.geographicalLocations()
    res.send(data)
})
app.get(('/customerscohorts'), async(req, res) => {
    const data = await MongoDB.customerCohorts()
    res.send(data)
})




const PORT = process.env.PORT || 4040;
app.listen(PORT, () => {
    console.log(`Server running on ${PORT}`);
});

MongoDB.connectDB()