const {ApolloServer, gql} = require("apollo-server-express");
const express = require('express');
const {GraphQLScalarType} = require("graphql");
const {Kind} = require("graphql/language");
const mongoose = require('mongoose');
const StrUtil = require('@supercharge/strings')
require('dotenv').config();

mongoose.connect(process.env.DB_CONN,
    {useNewUrlParser: true, useUnifiedTopology: true, keepAlive: true}).then(
    () => {
        /** ready to use. The `mongoose.connect()` promise resolves to mongoose instance. */
        console.log("Connected to Database")
    },
    err => {
        /** handle initial connection error */
        console.log("Error Connection DB" + err)
    }
);

const db = mongoose.connection;

const axios = require('axios').default;
const schedule = require('node-schedule');

const walletSchema = new mongoose.Schema({
    dfi: Number,
    btcdfi: Number,
    ethdfi: Number,
    ltcdfi: Number,
    dogedfi: Number,
    usdtdfi: Number,

    dfiInStaking: Number,

    // BTC Pool
    btcInBtcPool: Number,
    btc: Number,
    dfiInBtcPool: Number,

    // ETH Pool
    ethInEthPool: Number,
    eth: Number,
    dfiInEthPool: Number,

    // USDT Pool
    usdtInUsdtPool: Number,
    usdt: Number,
    dfiInUsdtPool: Number,

    // LTC Pool
    ltcInLtcPool: Number,
    ltc: Number,
    dfiInLtcPool: Number,

    // DOGE Pool
    dogeInDogePool: Number,
    doge: Number,
    dfiInDogePool: Number
});


const userSchema = new mongoose.Schema({
    key: String,
    createdDate: Date,
    addresses: [String],
    wallet: walletSchema,
});

const poolDefinition = {
    symbol: String,
    poolId: String,
    name: String,
    pair: String,
    logo: String,
    customRewards: [String],
    pairLink: String,
    apy: Number,
    apr: Number,
    idTokenA: String,
    idTokenB: String,
    totalStaked: Number,
    poolPairId: String,
    reserveA: String,
    reserveB: String,
    volumeA: Number,
    volumeB: Number,
    tokenASymbol: String,
    tokenBSymbol: String,
    priceA: Number,
    priceB: Number,
    totalLiquidityLpToken: Number,
    date: Date,
    totalLiquidity: Number,
    rewardPct: Number,
    commission: Number
}

const poolFarming = new mongoose.Schema({
    pools: [poolDefinition],
    tvl: Number,
    date: Date
});

const poolBTCSchema = new mongoose.Schema(poolDefinition);
const poolETHSchema = new mongoose.Schema(poolDefinition);
const poolUSDTSchema = new mongoose.Schema(poolDefinition);
const poolLTCSchema = new mongoose.Schema(poolDefinition);
const poolBCHSchema = new mongoose.Schema(poolDefinition);
const poolDOGESchema = new mongoose.Schema(poolDefinition);
const poolFarmingSchema = new mongoose.Schema(poolFarming);

const User = mongoose.model("User", userSchema);

const PoolBTC = mongoose.model("PoolBTC", poolBTCSchema);
const PoolETH = mongoose.model("PoolETH", poolETHSchema);
const PoolUSDT = mongoose.model("PoolUSDT", poolUSDTSchema);
const PoolLTC = mongoose.model("PoolLTC", poolLTCSchema);
const PoolBCH = mongoose.model("PoolBCH", poolBCHSchema);
const PoolDOGE = mongoose.model("PoolDOGE", poolDOGESchema);
const PoolFarming = mongoose.model("PoolFarming", poolFarmingSchema);

// gql`` parses your string into an AST
const typeDefs = gql`
    scalar Date

    type Wallet {
        dfi: Float

        dfiInStaking: Float
       
        btcdfi : Float
        ethdfi: Float
        ltcdfi: Float
        dogedfi: Float
        usdtdfi: Float

        # BTC Pool
        btcInBtcPool: Float
        btc: Float
        dfiInBtcPool: Float

        # ETH Pool
        ethInEthPool: Float
        eth: Float
        dfiInEthPool: Float

        # USDT Pool
        usdtInUsdtPool: Float
        usdt: Float
        dfiInUsdtPool: Float

        # LTC Pool
        ltcInLtcPool: Float
        ltc: Float
        dfiInLtcPool: Float

        # DOGE Pool
        dogeInDogePool: Float
        doge: Float
        dfiInDogePool: Float
    }
    
    type Pool {
        id: ID!
        symbol: String
        poolId: String
        name: String
        pair: String
        logo: String
        customRewards: [String]
        pairLink: String
        apy: Float
        apr: Float
        idTokenA: String
        idTokenB: String
        totalStaked: Float
        poolPairId: String
        reserveA: String
        reserveB: String
        volumeA: Float
        volumeB: Float
        tokenASymbol: String
        tokenBSymbol: String
        priceA: Float
        priceB: Float
        totalLiquidityLpToken: Float
        date: Date,
        totalLiquidity: Float
        rewardPct: Float
        commission: Float
    }
    
    type PoolList {
        pools: [Pool]
        tvl: Float
        date: Date
    }
    
    type User {
        id: ID!
        key: String!
        createdDate: Date
        wallet: Wallet
        addresses: [String]
    }

    type Query {
        users: [User]
        user(id: String): User
        userByKey(key: String): User
        getAuthKey: String
        poolbtc: [Pool]
        pooleth: [Pool]
        poolltc: [Pool]
        poolusdt: [Pool]
        pooldoge: [Pool]
        poolbch: [Pool]
        poolfarming: [PoolList]
    }

    input WalletInput {
        dfi: Float

        btcdfi : Float
        ethdfi: Float
        ltcdfi: Float
        dogedfi: Float
        usdtdfi: Float
        
        dfiInStaking: Float

        # BTC Pool
        btcInBtcPool: Float
        btc: Float
        dfiInBtcPool: Float

        # ETH Pool
        ethInEthPool: Float
        eth: Float
        dfiInEthPool: Float

        # USDT Pool
        usdtInUsdtPool: Float
        usdt: Float
        dfiInUsdtPool: Float

        # LTC Pool
        ltcInLtcPool: Float
        ltc: Float
        dfiInLtcPool: Float

        # DOGE Pool
        dogeInDogePool: Float
        doge: Float
        dfiInDogePool: Float
    }
    
    input UserInput {
        wallet: WalletInput
        addresses: [String]
    }

    input UserUpdateInput {
        key: String!
        wallet: WalletInput
        addresses: [String]
    }

    input UserAddressInput {
        key: String!
        address: String!
    }

    type Mutation {
        addUser(user: UserInput): User
        updateUser(user: UserUpdateInput): User
        addUserAddress(user: UserAddressInput): User
        updateWallet(wallet: WalletInput): User
    }
`;

async function findUserByKey(key) {
    return User.findOne({key: key});
}

const resolvers = {
    Query: {
        users: async () => {
            try {
                return await User.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },

        user: async (obj, {id}) => {
            try {
                return await User.findById(id);
            } catch (e) {
                console.log("e", e);
                return {};
            }
        },
        userByKey: async (obj, {key}) => {
            try {
                return await findUserByKey(key);
            } catch (e) {
                console.log("e", e);
                return {};
            }
        },
        getAuthKey: async () => {
            try {
                return  StrUtil.random(8)
            } catch (e) {
                console.log("e", e);
                return {};
            }
        },
        poolbtc: async () => {
            try {
                return await PoolBTC.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        pooleth: async () => {
            try {
                return await PoolETH.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        poolltc: async () => {
            try {
                return await PoolLTC.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        poolusdt: async () => {
            try {
                return await PoolUSDT.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        poolbch: async () => {
            try {
                return await PoolUSDT.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        pooldoge: async () => {
            try {
                return await PoolDOGE.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        poolfarming: async () => {
            try {
                return await PoolFarming.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        }
    },

    Mutation: {
        addUser: async (obj, {user}, {userId}) => {
            try {
                if (!userId) {
                    return null;
                }

                const millisecondsBefore = new Date().getTime();

                const createdUser = await User.create({
                    createdDate: new Date(),
                    addresses: user.addresses,
                    key: StrUtil.random(8),
                    wallet : Object.assign({}, user.wallet)
                });

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                console.log("Add User called took " + msTime + " ms.");
                return createdUser;

            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        updateUser: async (obj, {user}, {userId}) => {
            try {
                if (!userId) {
                    return null;
                }

                const millisecondsBefore = new Date().getTime();

                const userLoaded =  await findUserByKey(user.key);
                if (!userLoaded) {
                    return null;
                }

                userLoaded.addresses = user.addresses;
                userLoaded.wallet = Object.assign({}, user.wallet);

                const saved =  await userLoaded.save();

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                console.log("Update User called took " + msTime + " ms.");
                return saved;

            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        addUserAddress: async (obj, {user}, {userId}) => {
            try {
                if (!userId) {
                    return null;
                }

                const foundUser = await findUserByKey(user.key);

                if (!foundUser) {
                    return null;
                }
                // only when  in database
                if (foundUser.addresses && foundUser.addresses.indexOf(user.address) === -1) {
                    const newAddresses = foundUser.addresses.concat(user.address);
                    foundUser.addresses = newAddresses;
                    return await foundUser.save();
                }

                return foundUser;

            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        updateWallet: async (obj, {wallet}, {userId}) => {
            try {
                if (!userId) {
                    return null;
                }
                const foundUser = await User.findOne({key: wallet.key});

                // only when  in database
                if (!foundUser) {
                    return null;
                }

                foundUser.wallet = Object.assign({}, wallet);
                return await foundUser.save();

            } catch (e) {
                console.log("e", e);
                return [];
            }
        }
    },

    Date: new GraphQLScalarType({
        name: "Date",
        description: "it's a date, deal with it",
        parseValue(value) {
            // value from the client
            return new Date(value);
        },
        serialize(value) {
            // value sent to the client
            return value.getTime();
        },
        parseLiteral(ast) {
            if (ast.kind === Kind.INT) {
                return new Date(ast.value);
            }
            return null;
        }
    })
};

function getPool(id) {
    return axios.get(process.env.POOL_API + id);
}

function getPoolFarming() {
    return axios.get(process.env.POOL_FARMING_API);
}

async function saveBTCPool(data) {

    const createdBTCPOOL = await PoolBTC.create(assignDataValue(data, new PoolBTC(), "5"));
    return createdBTCPOOL;
}

async function saveETHPool(data) {

    const createdETHPOOL = await PoolETH.create(assignDataValue(data, new PoolETH(), "4"));
    return createdETHPOOL;
}

async function saveLTCPool(data) {

    const createdLTCPOOL = await PoolLTC.create(assignDataValue(data, new PoolLTC(), "10"));
    return createdLTCPOOL;
}

async function saveUSDTPool(data) {

    const createdUSDTPool = await PoolUSDT.create(assignDataValue(data, new PoolUSDT(), "6"));
    return createdUSDTPool;
}

async function saveBCHPool(data) {

    const createdBCHPool = await PoolBCH.create(assignDataValue(data, new PoolBCH(), "12"));
    return createdBCHPool;
}

async function saveDOGEPool(data) {

    const createdDOGEPool = await PoolDOGE.create(assignDataValue(data, new PoolDOGE(), "8"));
    return createdDOGEPool;
}

async function saveFarmingPool(data) {
    const pools = [];
    data.pools.forEach(p => {
        pools.push(assignDataValue(p, {}, p.poolPairId))
    })
    const createdFarmingPool = await PoolFarming.create({pools, tvl: data.tvl, date: new Date()});
    return createdFarmingPool;
}

function assignDataValue(data, object, id) {

    object.date = new Date();
    object.poolId = id;
    object.poolPairId= data.poolPairId;
    object.apr = data.apr;
    object.name = data.name;
    object.pair = data.pair;
    object.logo = data.logo;
    object.customRewards = data.customRewards;
    object.pairLink = data.pairLink;
    object.apy = data.apy;
    object.idTokenA = data.idTokenA;
    object.idTokenB = data.idTokenB;
    object.totalStaked = data.totalStaked;
    object.poolPairId = data.poolPairId;
    object.reserveA = data.reserveA;
    object.reserveB = data.reserveB;
    object.volumeA = data.volumeA;
    object.volumeB = data.volumeB;
    object.tokenASymbol = data.tokenASymbol;
    object.tokenBSymbol = data.tokenBSymbol;
    object.priceA = data.priceA;
    object.priceB = data.priceB;
    object.totalLiquidityLpToken = data.totalLiquidityLpToken;
    object.totalLiquidity = data.totalLiquidity;
    object.rewardPct= data.rewardPct;
    object.commission = data.commission;
    object.symbol = data.symbol;
    return object;

}

const app = express();

console.log("JOB " + process.env.JOB_SCHEDULER_ON)
if (process.env.JOB_SCHEDULER_ON === "on") {
    schedule.scheduleJob(process.env.JOB_SCHEDULER_TURNUS, function () {
        const millisecondsBefore = new Date().getTime();

        Promise.all([getPool("5"), getPool("4"), getPool("6"), getPool("10"), getPool("8"), getPool("12"), getPoolFarming()])
            .then(function (results) {
               const btc = results[0];
                saveBTCPool(btc.data)
                    .catch(function (error) {
                        // handle error
                        console.log(error);
                    });

                const eth = results[1];
                saveETHPool(eth.data)
                    .catch(function (error) {
                        // handle error
                        console.log(error);
                    });
                const usdt = results[2];
                saveUSDTPool(usdt.data)
                    .catch(function (error) {
                        // handle error
                        console.log(error);
                    });
                const ltc = results[3];
                saveLTCPool(ltc.data)
                    .catch(function (error) {
                        // handle error
                        console.log(error);
                    });
                const doge = results[4];
                saveDOGEPool(doge.data)
                    .catch(function (error) {
                        // handle error
                        console.log(error);
                    });
                const bch = results[5];
                saveBCHPool(bch.data)
                    .catch(function (error) {
                        // handle error
                        console.log(error);
                    });
                const farming = results[6];
                saveFarmingPool(farming.data).catch(function (error) {
                    // handle error
                    console.log(error);
                });

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                console.log("Job executed time: " + new Date() + " in " + msTime + " ms.");

            }).catch(function (error) {
            // handle error
            console.log(error);
        })
            .then(function () {
                // always executed
            });


    });
}

const corsOptions = {
    origin: '*',
    credentials: true // <-- REQUIRED backend setting
};

const server = new ApolloServer({
    typeDefs,
    resolvers,
    introspection: true,
    playground: true,
    context: ({req}) => {
        const fakeUser = {
            userId: "helloImauser"
        };
        return {
            ...fakeUser
        };
    }
});

server.applyMiddleware({ app, cors: corsOptions });



app.listen({ port: 4000 }, () => {
    console.log(`🚀 Server ready at http://localhost:4000/graphql`)

}
);


