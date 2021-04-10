const {ApolloServer, gql} = require("apollo-server-express");
const express = require('express');
const {GraphQLScalarType} = require("graphql");
const {Kind} = require("graphql/language");
const mongoose = require('mongoose');
const StrUtil = require('@supercharge/strings')
require('dotenv').config();

const messageAuth = "This ist not public Query. You need to provide an auth Key";

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
const {GraphQLError} = require("graphql");

const walletSchema = new mongoose.Schema({
    dfi: Number,
    btcdfi: Number,
    ethdfi: Number,
    ltcdfi: Number,
    dogedfi: Number,
    usdtdfi: Number,
    bchdfi: Number,

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
    dfiInDogePool: Number,

    // BCH Pool
    bchInBchPool: Number,
    bch: Number,
    dfiInBchPool: Number
});


const userSchema = new mongoose.Schema({
    key: String,
    createdDate: Date,
    addresses: [String],
    wallet: walletSchema,
});

const userTransactionsSchema = new mongoose.Schema({
    key: String,
    type: String,
    date: Date,
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

const Rewards = {
    anchorPercent: Number,
    liquidityPoolPercent: Number,
    communityPercent: Number,
    total: Number,
    community: Number,
    minter: Number,
    anchorReward: Number,
    liquidityPool: Number

}

const Tokens = {
    max: Number,
    supply: {
        total: Number,
        circulation: Number,
        foundation: Number,
        community: Number
    }

}

const StatsSchema = new mongoose.Schema({
    blockHeight: Number,
    difficulty: Number,
    rewards: Rewards,
    tokens: Tokens
});



const poolBTCSchema = new mongoose.Schema(poolDefinition);
const poolETHSchema = new mongoose.Schema(poolDefinition);
const poolUSDTSchema = new mongoose.Schema(poolDefinition);
const poolLTCSchema = new mongoose.Schema(poolDefinition);
const poolBCHSchema = new mongoose.Schema(poolDefinition);
const poolDOGESchema = new mongoose.Schema(poolDefinition);
const poolFarmingSchema = new mongoose.Schema(poolFarming);

const User = mongoose.model("User", userSchema);
const UserTransaction = mongoose.model("UserTransaction", userTransactionsSchema);

const PoolBTC = mongoose.model("PoolBTC", poolBTCSchema);
const PoolETH = mongoose.model("PoolETH", poolETHSchema);
const PoolUSDT = mongoose.model("PoolUSDT", poolUSDTSchema);
const PoolLTC = mongoose.model("PoolLTC", poolLTCSchema);
const PoolBCH = mongoose.model("PoolBCH", poolBCHSchema);
const PoolDOGE = mongoose.model("PoolDOGE", poolDOGESchema);
const PoolFarming = mongoose.model("PoolFarming", poolFarmingSchema);
const Stats = mongoose.model("Stats", StatsSchema);

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
        bchdfi: Float

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

        # BCH Pool
        bchInBchPool: Float
        bch: Float
        dfiInBchPool: Float
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

    type UserTransaction {
        id: ID!
        key: String!
        date: Date
        type: String!
        wallet: Wallet
        addresses: [String]
    }

    type Rewards {
        anchorPercent: Float
        liquidityPoolPercent: Float
        communityPercent: Float
        total: Float
        community: Float
        minter: Float
        anchorReward: Float
        liquidityPool: Float
    }

    type Tokens  {
        max: Float
        supply: Supply
    }

    type Supply {
        total: Float
        circulation: Float
        foundation: Float
        community: Float

    }

    type Stats {
        id: ID!
        blockHeight: Float
        difficulty: Float
        rewards: Rewards
        tokens: Tokens
    }

    type Query {
        users: [User]
        user(id: String): User
        userByKey(key: String): User
        userTransactionsByKey(key: String): [UserTransaction]
        userTransactions: [UserTransaction]
        getAuthKey: String
        getPoolbtcHistory: [Pool]
        getPoolethHistory: [Pool]
        getPoolltcHistory: [Pool]
        getPoolusdtHistory: [Pool]
        getPooldogeHistory: [Pool]
        getPoolbchHistory: [Pool]
        getFarmingHistory: [PoolList]
        getStats: [Stats]
    }

    input WalletInput {
        dfi: Float

        btcdfi : Float
        ethdfi: Float
        ltcdfi: Float
        dogedfi: Float
        usdtdfi: Float
        bchdfi: Float
        
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

        # BCH Pool
        bchInBchPool: Float
        bch: Float
        dfiInBchPool: Float
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

async function findUserTransactionsByKey(key) {
    return UserTransaction.find({key: key});
}

function checkAuth(auth) {
    return !auth || process.env.AUTH !== auth;
}


const resolvers = {
    Query: {
        users: async (obj, {user}, {auth}) => {
            try {

                if (checkAuth(auth)) {
                    return new GraphQLError(messageAuth);
                }

                return await User.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        userTransactions: async (obj, {user}, {auth}) => {
            try {

                if (checkAuth(auth)) {
                    return new GraphQLError(messageAuth);
                }

                return await UserTransaction.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        user: async (obj, {id}, {auth}) => {
            try {

                if (checkAuth(auth)) {
                    return new GraphQLError(messageAuth);
                }

                return await User.findById(id);
            } catch (e) {
                console.log("e", e);
                return {};
            }
        },
        userByKey: async (obj, {key}, {auth}) => {
            try {

                return await findUserByKey(key);
            } catch (e) {
                console.log("e", e);
                return {};
            }
        },
        userTransactionsByKey: async (obj, {key}, {auth}) => {
            try {

                return await findUserTransactionsByKey(key);
            } catch (e) {
                console.log("e", e);
                return {};
            }
        },
        getAuthKey: async (obj, {key}, {auth}) => {
            try {

                return  StrUtil.random(8)
            } catch (e) {
                console.log("e", e);
                return {};
            }
        },
        getPoolbtcHistory: async () => {
            try {
                return await PoolBTC.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        getPoolethHistory: async () => {
            try {
                return await PoolETH.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        getPoolltcHistory: async () => {
            try {
                return await PoolLTC.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        getPoolusdtHistory: async () => {
            try {
                return await PoolUSDT.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        getPoolbchHistory: async () => {
            try {
                return await PoolUSDT.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        getPooldogeHistory: async () => {
            try {
                return await PoolDOGE.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        getFarmingHistory: async () => {
            try {
                return await PoolFarming.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        getStats: async () => {
            try {
                return await Stats.find();
            } catch (e) {
                console.log("e", e);
                return [];
            }
        }
    },

    Mutation: {
        addUser: async (obj, {user}, {auth}) => {
            try {

                const millisecondsBefore = new Date().getTime();

                const createdUser = await User.create({
                    createdDate: new Date(),
                    addresses: user.addresses,
                    key: StrUtil.random(8),
                    wallet: Object.assign({}, user.wallet)
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
        updateUser: async (obj, {user}, {auth}) => {
            try {

                const millisecondsBefore = new Date().getTime();

                const userLoaded =  await findUserByKey(user.key);
                if (!userLoaded) {
                    return null;
                }

                userLoaded.addresses = user.addresses;
                userLoaded.wallet = Object.assign({}, user.wallet);

                const saved =  await userLoaded.save();

                // save transaction
                await UserTransaction.create({
                    date: new Date(),
                    type: "UPDATE",
                    addresses: user.addresses,
                    key: user.key,
                    wallet: Object.assign({}, user.wallet)
                });

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                console.log("Update User called took " + msTime + " ms.");
                return saved;

            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        addUserAddress: async (obj, {user}, {auth}) => {
            try {

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
        updateWallet: async (obj, {wallet}, {auth}) => {
            try {

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

function getStats() {
    return axios.get(process.env.STATS_API);
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

async function saveStats(data) {

    const createdStats = await Stats.create(assignDataValueStats(data, new Stats()));
    return createdStats;
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

function assignDataValueStats(data, object) {

    object.blockHeight = data.blockHeight;
    object.difficulty= data.difficulty;
    object.rewards = Object.assign({}, data.rewards);
    object.tokens = Object.assign({}, data.tokens);

    return object;

}

const app = express();


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

                console.log("Pools Job executed time: " + new Date() + " in " + msTime + " ms.");

            }).catch(function (error) {
            // handle error
            if (error.response) {
                // Request made and server responded
                console.log("==================== ERROR in Call to API BEGIN ====================");
                console.log(error.response.data);
                console.log(error.response.status);
                console.log(error.response.statusText);
                console.log("==================== ERROR in Call to API END ====================");
            } else if (error.request) {
                // The request was made but no response was received
                console.log(error.request);
            } else {
                // Something happened in setting up the request that triggered an Error
                console.log('Error', error.message);
            }
        })
            .then(function () {
                // always executed
            });


    });
}

if (process.env.JOB_SCHEDULER_ON_STATS === "on") {
    schedule.scheduleJob(process.env.JOB_SCHEDULER_TURNUS_STATS, function () {
        const millisecondsBefore = new Date().getTime();

        getStats()
            .then((response) => {
                saveStats(response.data)
                    .catch(function (error) {
                        // handle error
                        console.log(error);
                    });

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                console.log("Stats Job executed time: " + new Date() + " in " + msTime + " ms.");
            })
            .catch(function (error) {
                // handle error
                if (error.response) {
                    // Request made and server responded
                    console.log("==================== ERROR in Call to API BEGIN ====================");
                    console.log(error.response.data);
                    console.log(error.response.status);
                    console.log(error.response.statusText);
                    console.log("==================== ERROR in Call to API END ====================");
                } else if (error.request) {
                    // The request was made but no response was received
                    console.log(error.request);
                } else {
                    // Something happened in setting up the request that triggered an Error
                    console.log('Error', error.message);
                }

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                console.log("Stats Job executed time: " + new Date() + " in " + msTime + " ms.");
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
    formatError: (err) => {
        // When debug return only message
        if (process.env.DEBUG === "off")
           return {message : err.message};

        return err;
    },
    introspection: true,
    playground: true,
    context: ({req}) => {
        const auth = {
            auth: req.header("secureKey")
        };
        return {
            ...auth
        };
    }
});

server.applyMiddleware({ app, cors: corsOptions });



app.listen({ port: 4000 }, () => {
        console.log(`🚀 Server ready at http://localhost:4000/graphql`)
        console.log("JOB Pools " + process.env.JOB_SCHEDULER_ON)
        console.log("JOB Stats " + process.env.JOB_SCHEDULER_ON_STATS)
        console.log("DEBUG " + process.env.DEBUG)

}
);


