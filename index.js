const { ApolloServer, gql } = require("apollo-server");
const { GraphQLScalarType } = require("graphql");
const { Kind } = require("graphql/language");
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

const userSchema = new mongoose.Schema({
    key: String,
    createdDate: Date,
    addresses: [String]
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
        walletManuel: Wallet
        addresses: [String]
    }

    type Query {
        users: [User]
        user(id: String): User
        userByKey(key: String): User
    }

    input WalletInput {
        dfi: Float
    }

    input UserInput {
        key: String!
        walletMan: WalletInput
        walletAuto: WalletInput
        addresses: [String]
    }

    input UserAddressInput {
        key: String!
        address: String!
    }

    type Mutation {
        addUser(user: UserInput): User
        addUserAddress(user: UserAddressInput): User
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

        user: async (obj, { id }) => {
            try {
                return  await User.findById(id);
            } catch (e) {
                console.log("e", e);
                return {};
            }
        },
        userByKey: async (obj, { key }) => {
            try {
                return await User.findOne({key: key});
            } catch (e) {
                console.log("e", e);
                return {};
            }
        }
    },

    Mutation: {
        addUser: async (obj, { user }, { userId }) => {
            try {
                if (userId) {
                    const foundUser = await User.findOne({key: user.key});
                    // only when not in database
                    if (!foundUser) {
                        console.log(user)

                        // Do mutation and of database stuff
                        const createdUser = await User.create({
                          createdDate: new Date(),
                          addresses: user.addresses,
                          key: user.key
                        });
                        return createdUser;

                    }
                    return null;
                }
                return [];
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
        addUserAddress: async (obj, { user }, { userId }) => {
            try {
                if (userId) {
                    const foundUser = await User.findOne({key: user.key});
                    // only when  in database
                    if (foundUser) {
                        foundUser.addresses.push(user.address);
                        return  await foundUser.save();

                    }
                    return null;
                }
                return [];
            } catch (e) {
                console.log("e", e);
                return [];
            }
        },
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
    context: ({ req }) => {
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
    .then(({ url }) => {
        console.log(`Server started at ${url}`);
    });

