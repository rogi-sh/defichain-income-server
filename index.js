const {ApolloServer, gql} = require("apollo-server");
const {GraphQLScalarType} = require("graphql");
const {Kind} = require("graphql/language");
const mongoose = require('mongoose');
require('dotenv').config();

mongoose.connect(process.env.DB_CONN,
    {useNewUrlParser: true, useUnifiedTopology: true, keepAlive: true}).then(
    () => {
        /** ready to use. The `mongoose.connect()` promise resolves to mongoose instance. */
        console.log("Connected")
    },
    err => {
        /** handle initial connection error */
        console.log("Error Connection DB" + err)
    }
);

const db = mongoose.connection;

const walletSchema = new mongoose.Schema({
    dfi: Number,
    btcdfi: Number,
    ethdfi: Number,
    ltcdfi: Number,
    dogedfi: Number,
    usdtdfi: Number,

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
    walletAuto: walletSchema,
    walletMan: walletSchema
});

const User = mongoose.model("User", userSchema);

// gql`` parses your string into an AST
const typeDefs = gql`
    scalar Date

    type Wallet {
        dfi: Float
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

    type User {
        id: ID!
        key: String!
        createdDate: Date
        walletAuto: Wallet
        walletMan: Wallet
        addresses: [String]
    }

    type Query {
        users: [User]
        user(id: String): User
        userByKey(key: String): User
    }

    input WalletInputAuto {
        key: String!
        
        dfi: Float
        btcdfi : Float
        ethdfi: Float
        ltcdfi: Float
        dogedfi: Float
        usdtdfi: Float

        btc: Float
        eth: Float
        usdt: Float
        ltc: Float
        doge: Float
    }

    input WalletInputMan {
        key: String!
        dfi: Float

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
        key: String!
        walletMan: WalletInputMan
        walletAuto: WalletInputAuto
        addresses: [String]
    }

    input UserAddressInput {
        key: String!
        address: String!
    }

    type Mutation {
        addUser(user: UserInput): User
        addUserAddress(user: UserAddressInput): User
        updateWalletAuto(wallet: WalletInputAuto): User
        updateWalletMan(wallet: WalletInputMan): User
    }
`;

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
                return await User.findOne({key: key});
            } catch (e) {
                console.log("e", e);
                return {};
            }
        }
    },

    Mutation: {
        addUser: async (obj, {user}, {userId}) => {
            try {
                if (!userId) {
                    return null;
                }

                const foundUser = await User.findOne({key: user.key});

                if (foundUser) {
                    return null;
                }
                // only when not in database
                const createdUser = await User.create({
                    createdDate: new Date(),
                    addresses: user.addresses,
                    key: user.key,
                    walletAuto : Object.assign({}, user.walletAuto),
                    walletMan: Object.assign({}, user.walletMan)
                });
                return createdUser;

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

                const foundUser = await User.findOne({key: user.key});

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
        updateWalletAuto: async (obj, {wallet}, {userId}) => {
            try {
                if (!userId) {
                    return null;
                }
                const foundUser = await User.findOne({key: wallet.key});

                // only when  in database
                if (!foundUser) {
                    return null;
                }

                foundUser.walletAuto = Object.assign({}, wallet);
                return await foundUser.save();

            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        updateWalletMan: async (obj, {wallet}, {userId}) => {
            try {
                if (!userId) {
                    return null;
                }
                const foundUser = await User.findOne({key: wallet.key});

                // only when  in database
                if (!foundUser) {
                    return null;
                }


                foundUser.walletMan = Object.assign({}, wallet);
                console.log(foundUser)
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

server
    .listen({
        port: process.env.PORT || 4000
    })
    .then(({url}) => {
        console.log(`Server started at ${url}`);
    });

