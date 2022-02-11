const {ApolloServer, gql} = require("apollo-server-express");
const express = require('express');
const {GraphQLScalarType} = require("graphql");
const {Kind} = require("graphql/language");
const mongoose = require('mongoose');
const StrUtil = require('@supercharge/strings')
require('dotenv').config();
const CorrelationComputing = require("calculate-correlation");
const {WhaleApiClient} = require('@defichain/whale-api-client');
const fromScriptHex = require('@defichain/jellyfish-address');
const nodemailer = require("nodemailer");

const messageAuth = "This ist not public Query. You need to provide an auth Key";

const winston = require('winston');
const mjml2html = require('mjml');

const {SeqTransport} = require('@datalust/winston-seq');

const client = new WhaleApiClient({
    url: 'https://ocean.defichain.com',
    timeout: 60000,
    version: 'v0',
    network: 'mainnet'
})

const mailer = nodemailer.createTransport({
    pool: true,
    maxConnections: 3,
    host: process.env.MAIL_SERVER,
    port: 587,
    secure: false, // true for 465, false for other ports
    auth: {
        user: process.env.MAIL_USER,
        pass: process.env.MAIL_PASSWORD,
    }});

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(  /* This is required to get errors to log with stack traces. See https://github.com/winstonjs/winston/issues/1498 */
        winston.format.errors({stack: true}),
        winston.format.json(),
    ),
    defaultMeta: { /* application: 'your-app-name' */},
    transports: [
        new winston.transports.Console({
            format: winston.format.simple(),
        }),
        new SeqTransport({
            serverUrl: "https://log.defichain-income.com",
            apiKey: "3hedtJfVKArc0DQWH9og",
            onError: (e => {
                console.error(e)
            }),
            handleExceptions: true,
            handleRejections: true,
        })
    ]
});

mongoose.connect(process.env.DB_CONN,
    {useNewUrlParser: true, useUnifiedTopology: true, keepAlive: true, dbName: "defichain"}).then(
    () => {
        /** ready to use. The `mongoose.connect()` promise resolves to mongoose instance. */
        logger.info("Connected to Database")
    },
    err => {
        /** handle initial connection error */
        logger.error("Error Connection DB", err)
    }
);

const db = mongoose.connection;

const axios = require('axios').default;
const schedule = require('node-schedule');
const {GraphQLError} = require("graphql");
const {MainNet} = require("@defichain/jellyfish-network/dist/Network");
const fs = require("fs");

const payingAddress = 'df1qdc79xa70as0a5d0pdtgdww7tu65c2ncu9v7k2k';

const walletSchema = new mongoose.Schema({
    dfi: Number,

    btcdfi: Number,
    ethdfi: Number,
    ltcdfi: Number,
    dogedfi: Number,
    usdtdfi: Number,
    usdcdfi: Number,
    bchdfi: Number,
    usddfi: Number,
    tslausd: Number,
    spyusd: Number,
    qqqusd: Number,
    pltrusd: Number,
    slvusd: Number,
    aaplusd: Number,
    gldusd: Number,
    gmeusd: Number,
    googlusd: Number,
    arkkusd: Number,
    babausd: Number,
    vnqusd: Number,
    urthusd: Number,
    tltusd: Number,
    pdbcusd: Number,
    amznusd: Number,
    nvdausd: Number,
    coinusd: Number,
    eemusd: Number,

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

    // USDC Pool
    usdcInUsdcPool: Number,
    usdc: Number,
    dfiInUsdcPool: Number,

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
    dfiInBchPool: Number,

    // USD Pool
    usdInUsdPool: Number,
    usd: Number,
    dfiInUsdPool: Number,

    // TSLA Pool
    tslaInTslaPool: Number,
    tsla: Number,
    usdInTslaPool: Number,

    // QQQ Pool
    qqqInQqqPool: Number,
    qqq: Number,
    usdInQqqPool: Number,

    // SPY Pool
    spyInSpyPool: Number,
    spy: Number,
    usdInSpyPool: Number,

    // PLTR Pool
    pltrInPltrPool: Number,
    pltr: Number,
    usdInPltrPool: Number,

    // SLV Pool
    slvInSlvPool: Number,
    slv: Number,
    usdInSlvPool: Number,

    // AAPL Pool
    aaplInAaplPool: Number,
    aapl: Number,
    usdInAaplPool: Number,

    // GLD Pool
    gldInGldPool: Number,
    gld: Number,
    usdInGldPool: Number,

    // GME Pool
    gmeInGmePool: Number,
    gme: Number,
    usdInGmePool: Number,

    // GOOGL Pool
    googlInGooglPool: Number,
    googl: Number,
    usdInGooglPool: Number,

    // ARKK Pool
    arkkInArkkPool: Number,
    arkk: Number,
    usdInArkkPool: Number,

    // BABA Pool
    babaInBabaPool: Number,
    baba: Number,
    usdInBabaPool: Number,

    // VNQ Pool
    vnqInVnqPool: Number,
    vnq: Number,
    usdInVnqPool: Number,

    // URTH Pool
    urthInUrthPool: Number,
    urth: Number,
    usdInUrthPool: Number,

    // TLT Pool
    tltInTltPool: Number,
    tlt: Number,
    usdInTltPool: Number,

    // PDBC Pool
    pdbcInPdbcPool: Number,
    pdbc: Number,
    usdInPdbcPool: Number,

    // AMZN Pool
    amznInAmznPool: Number,
    amzn: Number,
    usdInAmznPool: Number,

    // NVDA Pool
    nvdaInNvdaPool: Number,
    nvda: Number,
    usdInNvdaPool: Number,

    // COIN Pool
    coinInCoinPool: Number,
    coin: Number,
    usdInCoinPool: Number,

    // EEM Pool
    eemInEemPool: Number,
    eem: Number,
    usdInEemPool: Number
});

const newsletterSchema = new mongoose.Schema({
    email: String,
    payingAddress: String,
    status: String,
    subscribed: Date

});

const addressV2Definition = {
    addressId: String,
    masternode: Boolean,
    freezer: Number,
    name: String,
};

const userSchema = new mongoose.Schema({
    key: String,
    createdDate: Date,
    addresses: [String],
    addressesMasternodes: [String],
    adressesMasternodesFreezer5: [String],
    adressesMasternodesFreezer10: [String],
    addressesV2: [addressV2Definition],
    wallet: walletSchema,
    newsletter: newsletterSchema,
    totalValue: Number,
    totalValueIncomeDfi: Number,
    totalValueIncomeUsd: Number
});

const userTransactionsSchema = new mongoose.Schema({
    key: String,
    type: String,
    date: Date,
    addresses: [String],
    addressesMasternodes: [String],
    adressesMasternodesFreezer5: [String],
    adressesMasternodesFreezer10: [String],
    addressesV2: [addressV2Definition],
    wallet: walletSchema,
    newsletter: newsletterSchema
});

const userHistoryItemSchema = new mongoose.Schema({
    date: Date,
    totalValue: Number,
    totalValueIncomeDfi: Number,
    totalValueIncomeUsd: Number
});

const userHistorySchema = new mongoose.Schema({
    key: String,
    values: [userHistoryItemSchema]
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
    volumeA30: Number,
    volumeB30: Number,
    tokenASymbol: String,
    tokenBSymbol: String,
    priceA: Number,
    priceB: Number,
    totalLiquidityLpToken: Number,
    totalLiquidityUsd: Number,
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
    total: Number,
    community: Number,
    minter: Number,
    anchorReward: Number,
    liquidityPool: Number,
    burned: Number

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

const Tvl = {
    dex: Number,
    masternodes: Number,
    loan: Number,
    total: Number
}

const Burned = {
    address: Number,
    emission: Number,
    fee: Number,
    total: Number
}

const Count = {
    Blocks: Number,
    prices: Number,
    tokens: Number,
    masternodes: Number
}

const Price = {
    usd: Number,
    usdt: Number
}

const Masternodes = {
    locked: [
        {
            weeks: Number,
            count: Number,
            tvl: Number
        },
        {
            weeks: Number,
            count: Number,
            tvl: Number
        },
        {
            weeks: Number,
            count: Number,
            tvl: Number
        }
    ]
}

const Loan = {
    count: {
        collateralTokens: Number,
        loanTokens: Number,
        openAuctions: Number,
        openVaults: Number,
        schemes: Number
    },
    value: {
        collateral: Number,
        loan: Number
    }
}

const Net = {
    version: Number,
    subversion: String,
    protocolversion: Number
}

const StatsSchema = new mongoose.Schema({
    blockHeight: Number,
    difficulty: Number,
    rewards: Rewards,
    tokens: Tokens,
    tvl: Tvl,
    burned: Burned,
    count: Count,
    price: Price,
    masternodes: Masternodes,
    loan: Loan,
    net: Net
});


const poolBTCSchema = new mongoose.Schema(poolDefinition);
const poolETHSchema = new mongoose.Schema(poolDefinition);
const poolUSDTSchema = new mongoose.Schema(poolDefinition);
const poolUSDCSchema = new mongoose.Schema(poolDefinition);
const poolLTCSchema = new mongoose.Schema(poolDefinition);
const poolBCHSchema = new mongoose.Schema(poolDefinition);
const poolDOGESchema = new mongoose.Schema(poolDefinition);
const poolUSDSchema = new mongoose.Schema(poolDefinition);
const poolTSLASchema = new mongoose.Schema(poolDefinition);
const poolFarmingSchema = new mongoose.Schema(poolFarming);

const User = mongoose.model("User", userSchema);
const UserTransaction = mongoose.model("UserTransaction", userTransactionsSchema);
const UserHistory = mongoose.model("UserHistory", userHistorySchema);

const PoolBTC = mongoose.model("PoolBTC", poolBTCSchema);
const PoolETH = mongoose.model("PoolETH", poolETHSchema);
const PoolUSDT = mongoose.model("PoolUSDT", poolUSDTSchema);
const PoolUSDC = mongoose.model("PoolUSDC", poolUSDCSchema);
const PoolLTC = mongoose.model("PoolLTC", poolLTCSchema);
const PoolBCH = mongoose.model("PoolBCH", poolBCHSchema);
const PoolDOGE = mongoose.model("PoolDOGE", poolDOGESchema);
const PoolUSD = mongoose.model("PoolUSD", poolUSDSchema);
const PoolTSLA = mongoose.model("PoolTSLA", poolTSLASchema);
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
        usdcdfi: Float
        bchdfi: Float
        usddfi: Float
        tslausd: Float
        spyusd: Float
        qqqusd: Float
        pltrusd: Float
        slvusd: Float
        aaplusd: Float
        gldusd: Float
        gmeusd: Float
        googlusd: Float
        arkkusd: Float
        babausd: Float
        vnqusd: Float
        urthusd: Float
        tltusd: Float
        pdbcusd: Float
        amznusd: Float
        nvdausd: Float
        coinusd: Float
        eemusd: Float

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

        # USDC Pool
        usdcInUsdcPool: Float
        usdc: Float
        dfiInUsdcPool: Float

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

        # USD Pool
        usdInUsdPool: Float
        usd: Float
        dfiInUsdPool: Float

        # TSLA Pool
        tslaInTslaPool: Float
        tsla: Float
        usdInTslaPool: Float

        # QQQ Pool
        qqqInQqqPool: Float
        qqq: Float
        usdInQqqPool: Float

        # SPY Pool
        spyInSpyPool: Float
        spy: Float
        usdInSpyPool: Float

        # PLTR Pool
        pltrInPltrPool: Float
        pltr: Float
        usdInPltrPool: Float

        # SLV Pool
        slvInSlvPool: Float
        slv: Float
        usdInSlvPool: Float

        # AAPL Pool
        aaplInAaplPool: Float
        aapl: Float
        usdInAaplPool: Float

        # GLD Pool
        gldInGldPool: Float
        gld: Float
        usdInGldPool: Float

        # GME Pool
        gmeInGmePool: Float
        gme: Float
        usdInGmePool: Float

        # GOOGL Pool
        googlInGooglPool: Float
        googl: Float
        usdInGooglPool: Float

        # ARKK Pool
        arkkInArkkPool: Float
        arkk: Float
        usdInArkkPool: Float

        # BABA Pool
        babaInBabaPool: Float
        baba: Float
        usdInBabaPool: Float

        # VNQ Pool
        vnqInVnqPool: Float
        vnq: Float
        usdInVnqPool: Float

        # URTH Pool
        urthInUrthPool: Float
        urth: Float
        usdInUrthPool: Float

        # TLT Pool
        tltInTltPool: Float
        tlt: Float
        usdInTltPool: Float

        # PDBC Pool
        pdbcInPdbcPool: Float
        pdbc: Float
        usdInPdbcPool: Float
        
        # AMZN Pool
        amznInAmznPool: Float
        amzn: Float
        usdInAmznPool: Float

        # NVDA Pool
        nvdaInNvdaPool: Float
        nvda: Float
        usdInNvdaPool: Float

        # COIN Pool
        coinInCoinPool: Float
        coin: Float
        usdInCoinPool: Float

        # EEM Pool
        eemInEemPool: Float
        eem: Float
        usdInEemPool: Float
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
        volumeA30: Float
        volumeB30: Float
        tokenASymbol: String
        tokenBSymbol: String
        priceA: Float
        priceB: Float
        totalLiquidityLpToken: Float
        totalLiquidityUsd: Float
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
    
    type Newsletter {
        email: String
        payingAddress: String
        status: String
        subscribed: Date
    }
    
    type AddressV2 {
        addressId: String
        masternode: Boolean
        freezer: Float
        name: String
    }
    
    type User {
        id: ID!
        key: String!
        createdDate: Date
        wallet: Wallet
        newsletter: Newsletter
        addresses: [String]
        addressesMasternodes: [String]
        adressesMasternodesFreezer5: [String]
        adressesMasternodesFreezer10: [String]
        addressesV2: [AddressV2]
        totalValue: Float
        totalValueIncomeDfi: Float
        totalValueIncomeUsd: Float
    }
    
    type UserHistoryItem {
        date: Date
        totalValue: Float
        totalValueIncomeDfi: Float
        totalValueIncomeUsd: Float
        
    }

    type UserHistory {
        key: String!
        values: [UserHistoryItem]
    }

    type UserTransaction {
        id: ID!
        key: String!
        date: Date
        type: String!
        wallet: Wallet
        newsletter: Newsletter
        addresses: [String]
        addressesMasternodes: [String]
        adressesMasternodesFreezer5: [String]
        adressesMasternodesFreezer10: [String]
        addressesV2: [AddressV2]
    }

    type Rewards {
        total: Float
        community: Float
        minter: Float
        anchorReward: Float
        liquidityPool: Float
        burned: Float
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
        tokens: Tokens,
        tvl: Tvl,
        burned: Burned,
        count: Count,
        price: Price,
        masternodes: Masternodes,
        loan: Loan,
        net: Net
    }

    type Tvl {
        dex: Float,
        masternodes: Float,
        loan: Float,
        total: Float
    }

    type Burned  {
        address: Float,
        emission: Float,
        fee: Float,
        total: Float
    }

    type Count {
        Blocks: Float,
        prices: Float,
        tokens: Float,
        masternodes: Float
    }

    type Price {
        usd: Float,
        usdt: Float
    }
    
    type LockedMasternodes {
        weeks: Float,
        count: Float,
        tvl: Float
    }

    type Masternodes {
        locked: [LockedMasternodes]
    }
    
    type LoanCount {
        collateralTokens: Float,
        loanTokens: Float,
        openAuctions: Float,
        openVaults: Float,
        schemes: Float
    }
    
    type LoanValue {
        collateral: Float,
        loan: Float
    }

    type Loan {
        count: LoanCount,
        value: LoanValue
    }

    type Net {
        version: Float,
        subversion: String,
        protocolversion: Float
    }
    
    type Statistics {
        users: Float,
        addresses: Float,
        addressesMasternodes: Float,
        visits: Float
    }
    
    type ExchangeStatus {
        bittrexStatus: String,
        bittrexNotice: String,
        kucoinStatusDeposit: Boolean,
        kucoinStatusWithdraw: Boolean,
        dfxBuy: String,
        dfxSell: String,
        dfxStaking: String
    }

    type Correlation {
        btcPool: Float
        ethPool: Float
        ltcPool: Float
        dogePool: Float
        bchPool: Float
        usdtPool: Float
        usdcPool: Float
        usdPool: Float
        tslaPool: Float

        btcPricesDex: [Float]
        ethPricesDex: [Float]
        ltcPricesDex: [Float]
        dogePricesDex: [Float]
        bchPricesDex: [Float]
        usdtPricesDex: [Float]
        usdcPricesDex: [Float]
        usdPricesDex: [Float]
        tslaPricesDex: [Float]
        dfiPricesDex: [Float]
    }

    type Query {
        users: [User]
        user(id: String): User
        userByKey(key: String): User
        userHistoryByKey(key: String): UserHistory
        userTransactionsByKey(key: String): [UserTransaction]
        userTransactions: [UserTransaction]
        getAuthKey: String
        getPoolbtcHistory(from: DateInput!, till: DateInput!): [Pool]
        getPoolethHistory(from: DateInput!, till: DateInput!): [Pool]
        getPoolltcHistory(from: DateInput!, till: DateInput!): [Pool]
        getPoolusdtHistory(from: DateInput!, till: DateInput!): [Pool]
        getPoolusdcHistory(from: DateInput!, till: DateInput!): [Pool]
        getPooldogeHistory(from: DateInput!, till: DateInput!): [Pool]
        getPoolbchHistory(from: DateInput!, till: DateInput!): [Pool]
        getPoolusdHistory(from: DateInput!, till: DateInput!): [Pool]
        getPooltslaHistory(from: DateInput!, till: DateInput!): [Pool]
        getFarmingHistory(from: DateInput!, till: DateInput!): [PoolList]
        getStats: [Stats]
        getCorrelation(days: Int): Correlation
        getStatisticsIncome: Statistics
        getExchangeStatus: ExchangeStatus
    }

    input WalletInput {
        dfi: Float

        btcdfi : Float
        ethdfi: Float
        ltcdfi: Float
        dogedfi: Float
        usdtdfi: Float
        usdcdfi: Float
        bchdfi: Float
        usddfi: Float
        tslausd: Float
        spyusd: Float
        qqqusd: Float
        pltrusd: Float
        slvusd: Float
        aaplusd: Float
        gldusd: Float
        gmeusd: Float
        googlusd: Float
        arkkusd: Float
        babausd: Float
        vnqusd: Float
        urthusd: Float
        tltusd: Float
        pdbcusd: Float
        amznusd: Float
        nvdausd: Float
        coinusd: Float
        eemusd: Float
        
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

        # USDC Pool
        usdcInUsdcPool: Float
        usdc: Float
        dfiInUsdcPool: Float

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

        # USD Pool
        usdInUsdPool: Float
        usd: Float
        dfiInUsdPool: Float

        # TSLA Pool
        tslaInTslaPool: Float
        tsla: Float
        usdInTslaPool: Float

        # QQQ Pool
        qqqInQqqPool: Float
        qqq: Float
        usdInQqqPool: Float

        # SPY Pool
        spyInSpyPool: Float
        spy: Float
        usdInSpyPool: Float

        # PLTR Pool
        pltrInPltrPool: Float
        pltr: Float
        usdInPltrPool: Float

        # SLV Pool
        slvInSlvPool: Float
        slv: Float
        usdInSlvPool: Float

        # AAPL Pool
        aaplInAaplPool: Float
        aapl: Float
        usdInAaplPool: Float

        # GLD Pool
        gldInGldPool: Float
        gld: Float
        usdInGldPool: Float

        # GME Pool
        gmeInGmePool: Float
        gme: Float
        usdInGmePool: Float

        # GOOGL Pool
        googlInGooglPool: Float
        googl: Float
        usdInGooglPool: Float

        # ARKK Pool
        arkkInArkkPool: Float
        arkk: Float
        usdInArkkPool: Float

        # BABA Pool
        babaInBabaPool: Float
        baba: Float
        usdInBabaPool: Float

        # VNQ Pool
        vnqInVnqPool: Float
        vnq: Float
        usdInVnqPool: Float

        # URTH Pool
        urthInUrthPool: Float
        urth: Float
        usdInUrthPool: Float

        # TLT Pool
        tltInTltPool: Float
        tlt: Float
        usdInTltPool: Float

        # PDBC Pool
        pdbcInPdbcPool: Float
        pdbc: Float
        usdInPdbcPool: Float
        
        # AMZN Pool
        amznInAmznPool: Float
        amzn: Float
        usdInAmznPool: Float

        # NVDA Pool
        nvdaInNvdaPool: Float
        nvda: Float
        usdInNvdaPool: Float

        # COIN Pool
        coinInCoinPool: Float
        coin: Float
        usdInCoinPool: Float

        # EEM Pool
        eemInEemPool: Float
        eem: Float
        usdInEemPool: Float        
    }
    
    input AddressV2Input {
        addressId: String
        masternode: Boolean
        freezer: Float
        name: String
    }
    
    input UserInput {
        wallet: WalletInput
        addresses: [String]
        addressesMasternodes: [String]
        adressesMasternodesFreezer5: [String]
        adressesMasternodesFreezer10: [String]
        addressesV2: [AddressV2Input]
        totalValue: Float
        totalValueIncomeDfi: Float
        totalValueIncomeUsd: Float
    }

    input UserUpdateInput {
        key: String!
        wallet: WalletInput
        addresses: [String]
        addressesMasternodes: [String]
        adressesMasternodesFreezer5: [String]
        adressesMasternodesFreezer10: [String]
        addressesV2: [AddressV2Input]
        totalValue: Float
        totalValueIncomeDfi: Float
        totalValueIncomeUsd: Float
    }
    
    input UserUpdateNewsletterInput {
        key: String!
        email: String!
        payingAddress: String
    }

    input DateInput {
        year: Int!
        month: Int!
        day: Int!
        hour: Int!
        min: Int!
        s: Int!
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
        updateUserNewsletter(user: UserUpdateNewsletterInput): User
    }
`;

async function findUserByKey(key) {
    return User.findOne({key: key});
}

async function findHistoryByKey(key) {
    return UserHistory.findOne({key: key});
}

async function findUserTransactionsByKey(key) {
    return UserTransaction.find({key: key}).sort({_id: -1}).limit(10).lean();
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

                return await User.find().lean();
            } catch (e) {
                logger.error("users", e);
                return [];
            }
        },
        userTransactions: async (obj, {user}, {auth}) => {
            try {

                if (checkAuth(auth)) {
                    return new GraphQLError(messageAuth);
                }

                return await UserTransaction.find().lean();
            } catch (e) {
                logger.error("userTransactions", e);
                return [];
            }
        },
        user: async (obj, {id}, {auth}) => {
            try {

                if (checkAuth(auth)) {
                    return new GraphQLError(messageAuth);
                }

                return await User.findById(id).lean();
            } catch (e) {
                logger.error("user", e);
                return {};
            }
        },
        userByKey: async (obj, {key}, {auth}) => {
            try {
                const millisecondsBefore = new Date().getTime();

                const user = await findUserByKey(key);

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;
                logger.info("Login " + new Date() + " called took " + msTime + " ms.");

                return user;
            } catch (e) {
                logger.error("userByKey", e);
                return {};
            }
        },
        userHistoryByKey: async (obj, {key}, {auth}) => {
            try {
                const millisecondsBefore = new Date().getTime();

                const userHistory = await findHistoryByKey(key);

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;
                logger.info("UserHistory " + new Date() + " called took " + msTime + " ms.");

                return userHistory;
            } catch (e) {
                logger.error("userHistoryByKey", e);
                return {};
            }
        },
        userTransactionsByKey: async (obj, {key}, {auth}) => {
            try {
                const millisecondsBefore = new Date().getTime();

                const userTransactions = await findUserTransactionsByKey(key);

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;
                logger.info("userTransactions  " + new Date() + " called took " + msTime + " ms.");

                return userTransactions;

            } catch (e) {
                logger.error("userTransactionsByKey", e);
                return {};
            }
        },
        getAuthKey: async (obj, {key}, {auth}) => {
            try {

                return StrUtil.random(16)
            } catch (e) {
                logger.error("getAuthKey", e);
                return {};
            }
        },
        getStatisticsIncome: async () => {
            try {

                const millisecondsBefore = new Date().getTime();

                const users = await User.find().lean();
                let usersCount = 0;
                let addresses = 0;
                let addressesMasternodes = 0;
                users.forEach(u => {
                    addresses += u.addresses ? u.addresses?.length: 0;
                    addressesMasternodes += u.addressesMasternodes ? u.addressesMasternodes?.length: 0;
                    usersCount += ((u.addresses && u.addresses.length > 0) || (u.addressesMasternodes && u.addressesMasternodes.length > 0)
                    || u.dfiInStaking > 0) ? 1 : 0;
                });

                let visits = 0;

                await axios.all([
                    getVisitors(),
                ])
                    .then(axios.spread((response) => {

                        const visitsValues = Object.values(response.data);
                        visits = Math.round((visitsValues[0] + visitsValues[1] + visitsValues[2]
                            + visitsValues[3] + visitsValues[4]) / 5);

                    }))
                    .catch(function (error) {
                        // handle error
                        if (error.response) {
                            // Request made and server responded
                            logger.error("==================== ERROR VisitsSummary in Call to API BEGIN ====================");
                            logger.error("getStatisticsIncome", error.response.data);
                            logger.error("getStatisticsIncome", error.response.status);
                            logger.error("getStatisticsIncome", error.response.statusText);
                            logger.error("==================== ERROR VisitsSummary in Call to API END ====================");
                        } else if (error.request) {
                            // The request was made but no response was received
                            logger.error("getStatisticsIncome", error.request);
                        } else {
                            // Something happened in setting up the request that triggered an Error
                            logger.error('getStatisticsIncome', error.message);
                        }
                    });

                const incomeStatistics = {
                    users: usersCount,
                    addresses: addresses,
                    addressesMasternodes: addressesMasternodes,
                    visits: visits
                };

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;
                logger.info("Income Statistics called took " + msTime + " ms.");
                return incomeStatistics;

            } catch (e) {
                logger.error("getStatisticsIncome", e);
                return {};
            }
        },
        getExchangeStatus: async () => {
            try {

                const millisecondsBefore = new Date().getTime();

                let bittrexStatus;
                let bittrexNotice;
                let kucoinStatusDeposit;
                let kucoinStatusWithdraw;
                let dfxBuy;
                let dfxSell;
                let dfxStaking

                await axios.all([
                    getStatusBittrex(), getStatusKucoin(), getStatusDfx()
                ])
                    .then(axios.spread((response, response2, response3) => {

                        bittrexStatus = response.data.status;
                        bittrexNotice = response.data.notice;
                        kucoinStatusDeposit = response2.data.data.isDepositEnabled;
                        kucoinStatusWithdraw = response2.data.data.isWithdrawEnabled;
                        dfxBuy = response3.data.buy;
                        dfxSell = response3.data.sell;
                        dfxStaking = response3.data.staking;

                    }))
                    .catch(function (error) {
                        // handle error
                        if (error.response) {
                            // Request made and server responded
                            logger.error("==================== ERROR Exchange Status in Call to API BEGIN ====================");
                            logger.error("getExchangeStatus", error.response.data);
                            logger.error("getExchangeStatus", error.response.status);
                            logger.error("getExchangeStatus", error.response.statusText);
                            logger.error("==================== ERROR Exchange Status in Call to API END ====================");
                        } else if (error.request) {
                            // The request was made but no response was received
                            logger.error("getExchangeStatus", error.request);
                        } else {
                            // Something happened in setting up the request that triggered an Error
                            logger.error('getExchangeStatus', error.message);
                        }
                    });

                const exchangeStatus = {
                    bittrexStatus: bittrexStatus,
                    bittrexNotice: bittrexNotice,
                    kucoinStatusDeposit: kucoinStatusDeposit,
                    kucoinStatusWithdraw: kucoinStatusWithdraw,
                    dfxBuy: dfxBuy,
                    dfxSell: dfxSell,
                    dfxStaking: dfxStaking
                };

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;
                logger.info("Exchange Status called took " + msTime + " ms.");
                return exchangeStatus;

            } catch (e) {
                logger.error("getExchangeStatus", e);
                return {};
            }
        },
        getPoolbtcHistory: async (obj, {from, till}, {auth}) => {
            try {

                const fromDate = new Date(Date.UTC(from.year, from.month - 1, from.day, from.hour, from.min, from.s, 0));
                const tillDate = new Date(Date.UTC(till.year, till.month - 1, till.day, till.hour, till.min, till.s, 0));

                return await PoolBTC.find({
                    date: {'$gte': fromDate, '$lte': tillDate}
                }).lean();
            } catch (e) {
                logger.error("getPoolbtcHistory", e);
                return [];
            }
        },
        getPoolethHistory: async (obj, {from, till}, {auth}) => {
            try {
                const fromDate = new Date(Date.UTC(from.year, from.month - 1, from.day, from.hour, from.min, from.s, 0));
                const tillDate = new Date(Date.UTC(till.year, till.month - 1, till.day, till.hour, till.min, till.s, 0));

                return await PoolETH.find({
                    date: {'$gte': fromDate, '$lte': tillDate}
                }).lean();
            } catch (e) {
                logger.error("getPoolethHistory", e);
                return [];
            }
        },
        getPoolltcHistory: async (obj, {from, till}, {auth}) => {
            try {
                const fromDate = new Date(Date.UTC(from.year, from.month - 1, from.day, from.hour, from.min, from.s, 0));
                const tillDate = new Date(Date.UTC(till.year, till.month - 1, till.day, till.hour, till.min, till.s, 0));

                return await PoolLTC.find({
                    date: {'$gte': fromDate, '$lte': tillDate}
                }).lean();
            } catch (e) {
                logger.error("getPoolltcHistory", e);
                return [];
            }
        },
        getPoolusdtHistory: async (obj, {from, till}, {auth}) => {
            try {
                const fromDate = new Date(Date.UTC(from.year, from.month - 1, from.day, from.hour, from.min, from.s, 0));
                const tillDate = new Date(Date.UTC(till.year, till.month - 1, till.day, till.hour, till.min, till.s, 0));

                return await PoolUSDT.find({
                    date: {'$gte': fromDate, '$lte': tillDate}
                }).lean();
            } catch (e) {
                logger.error("getPoolusdtHistory", e);
                return [];
            }
        },
        getPoolusdcHistory: async (obj, {from, till}, {auth}) => {
            try {
                const fromDate = new Date(Date.UTC(from.year, from.month - 1, from.day, from.hour, from.min, from.s, 0));
                const tillDate = new Date(Date.UTC(till.year, till.month - 1, till.day, till.hour, till.min, till.s, 0));

                return await PoolUSDC.find({
                    date: {'$gte': fromDate, '$lte': tillDate}
                }).lean();
            } catch (e) {
                logger.error("getPoolusdcHistory", e);
                return [];
            }
        },
        getPoolbchHistory: async (obj, {from, till}, {auth}) => {
            try {
                const fromDate = new Date(Date.UTC(from.year, from.month - 1, from.day, from.hour, from.min, from.s, 0));
                const tillDate = new Date(Date.UTC(till.year, till.month - 1, till.day, till.hour, till.min, till.s, 0));

                return await PoolBCH.find({
                    date: {'$gte': fromDate, '$lte': tillDate}
                }).lean();
            } catch (e) {
                logger.error("getPoolbchHistory", e);
                return [];
            }
        },
        getPoolusdHistory: async (obj, {from, till}, {auth}) => {
            try {
                const fromDate = new Date(Date.UTC(from.year, from.month - 1, from.day, from.hour, from.min, from.s, 0));
                const tillDate = new Date(Date.UTC(till.year, till.month - 1, till.day, till.hour, till.min, till.s, 0));

                return await PoolUSD.find({
                    date: {'$gte': fromDate, '$lte': tillDate}
                }).lean();
            } catch (e) {
                logger.error("getPoolusdHistory", e);
                return [];
            }
        },
        getPooltslaHistory: async (obj, {from, till}, {auth}) => {
            try {
                const fromDate = new Date(Date.UTC(from.year, from.month - 1, from.day, from.hour, from.min, from.s, 0));
                const tillDate = new Date(Date.UTC(till.year, till.month - 1, till.day, till.hour, till.min, till.s, 0));

                return await PoolTSLA.find({
                    date: {'$gte': fromDate, '$lte': tillDate}
                }).lean();
            } catch (e) {
                logger.error("getPooltslaHistory", e);
                return [];
            }
        },
        getPooldogeHistory: async (obj, {from, till}, {auth}) => {
            try {
                const fromDate = new Date(Date.UTC(from.year, from.month - 1, from.day, from.hour, from.min, from.s, 0));
                const tillDate = new Date(Date.UTC(till.year, till.month - 1, till.day, till.hour, till.min, till.s, 0));

                return await PoolDOGE.find({
                    date: {'$gte': fromDate, '$lte': tillDate}
                }).lean();
            } catch (e) {
                logger.error("getPooldogeHistory", e);
                return [];
            }
        },
        getFarmingHistory: async (obj, {from, till}, {auth}) => {
            try {
                const millisecondsBefore = new Date().getTime();

                const fromDate = new Date(Date.UTC(from.year, from.month - 1, from.day, from.hour, from.min, from.s, 0));
                const tillDate = new Date(Date.UTC(till.year, till.month - 1, till.day, till.hour, till.min, till.s, 0));

                let farming = await PoolFarming.find({date: {'$gte': fromDate, '$lte': tillDate}}).lean();

                const diff = Math.abs(fromDate.getTime() - tillDate.getTime());
                const diffDays = Math.ceil(diff / (1000 * 3600 * 24));
                if (diffDays > 3 && diffDays < 10) {
                    farming = farming.filter(f => f.date.getMinutes() === 0);
                } else if (diffDays >= 10 && diffDays < 30) {
                    farming = farming.filter(f => f.date.getHours() === 0 || f.date.getHours() === 6 ||
                        f.date.getHours() === 12 || f.date.getHours() === 18);
                } else if (diffDays >= 30) {
                    farming = farming.filter(f => f.date.getHours() === 3 || f.date.getHours() === 9);
                }

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;
                logger.info("Farming history for " + diffDays + " days called took " + msTime + " ms.");

                return farming;
            } catch (e) {
                logger.error("getFarmingHistory", e);
                return [];
            }
        },
        getStats: async () => {
            try {
                const millisecondsBefore = new Date().getTime();

                const stats = await Stats.find().lean();

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;
                logger.info("Stats defichain called took " + msTime + " ms.");

                return stats;
            } catch (e) {
                logger.error("getStats", e);
                return [];
            }
        },
        getCorrelation: async (obj, {days}, {auth}) => {
            try {
                return computeCorrelation(days);
            } catch (e) {
                logger.error("getCorrelation", e);
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
                    addressesMasternodes: user.addressesMasternodes,
                    adressesMasternodesFreezer5: user.adressesMasternodesFreezer5,
                    adressesMasternodesFreezer10: user.adressesMasternodesFreezer10,
                    addressesV2: user.addressesV2,
                    key: StrUtil.random(16),
                    wallet: Object.assign({}, user.wallet),
                    totalValue: user.totalValue,
                    totalValueIncomeDfi: user.totalValueIncomeDfi,
                    totalValueIncomeUsd: user.totalValueIncomeUsd
                });

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                logger.info("Add User called took " + msTime + " ms.");
                return createdUser;

            } catch (e) {
                logger.error("addUser", e);
                return [];
            }
        },
        updateUser: async (obj, {user}, {auth}) => {
            try {

                const millisecondsBefore = new Date().getTime();

                const userLoaded = await findUserByKey(user.key);
                if (!userLoaded) {
                    return null;
                }

                userLoaded.addresses = user.addresses;
                userLoaded.addressesMasternodes = user.addressesMasternodes;
                userLoaded.adressesMasternodesFreezer5 = user.adressesMasternodesFreezer5;
                userLoaded.adressesMasternodesFreezer10 = user.adressesMasternodesFreezer10;
                userLoaded.addressesV2 = user.addressesV2 ? user.addressesV2 : userLoaded.addressesV2;
                userLoaded.wallet = Object.assign({}, user.wallet);
                // Glitch protection if new value more than 10x and not negative
                if (user.totalValue > 0 && user.totalValue < userLoaded.totalValue * 10) {
                    userLoaded.totalValue = user.totalValue;
                }
                userLoaded.totalValueIncomeDfi = user.totalValueIncomeDfi;
                userLoaded.totalValueIncomeUsd = user.totalValueIncomeUsd;

                const saved = await userLoaded.save();

                // save transaction
                UserTransaction.create({
                    date: new Date(),
                    type: "UPDATE",
                    addresses: user.addresses,
                    addressesV2: userLoaded.addressesV2,
                    key: user.key,
                    wallet: Object.assign({}, user.wallet)
                });

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                logger.info("Update User called took " + msTime + " ms.");
                return saved;

            } catch (e) {
                logger.error("updateUser", e);
                return [];
            }
        },
        updateUserNewsletter: async (obj, {user}, {auth}) => {
            try {

                const millisecondsBefore = new Date().getTime();

                const userLoaded = await findUserByKey(user.key);
                if (!userLoaded) {
                    return null;
                }

                let subscribed = null;
                if (!userLoaded.newsletter || !userLoaded.newsletter.subscribed) {
                    subscribed = new Date();
                } else {
                    subscribed = userLoaded.newsletter.subscribed;
                }

                // kalkuliere Status
                let status = "";

                // 1. Subscribed
                if (user.email) {
                    status = "subscribed";
                }

                if (user.email && user.payingAddress) {
                    status = "subscribedWithAddress";
                }

                // 2. payed
                const payed = await checkNewsletterPayed(user.payingAddress);
                if (payed) {
                    status = "payed";
                }

                const newsletter = {
                    email: user.email,
                    payingAddress: user.payingAddress,
                    status: status,
                    subscribed: subscribed
                }

                userLoaded.newsletter = newsletter

                const saved = await userLoaded.save();

                // save transaction
                UserTransaction.create({
                    date: new Date(),
                    type: "UPDATE",
                    addresses: user.addresses,
                    addressesV2: user.addressesV2,
                    key: user.key,
                    wallet: Object.assign({}, user.wallet),
                    newsletter: newsletter
                });

                logger.info("Start Mail sending for update Newsletter to " + user.email);
                await sendUpdateNewsletterMail(user.email, user.payingAddress, status)

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                logger.info("Update User Newsletter called took " + msTime + " ms.");
                return saved;

            } catch (e) {
                logger.error("updateUserNewsletter", e);
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
                logger.error("addUserAddress", e);
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
                logger.error("updateWallet", e);
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
            return value;
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

function getStatsOcean() {
    return axios.get(process.env.STATS_OCEAN_API);
}

function getPoolPairs() {
    return axios.get(process.env.POOL_PAIRS_API);
}

function getVisitors() {
    return axios.get(process.env.VISITS_API);
}

function getStatusBittrex() {
    return axios.get(process.env.BITTREX_API);
}

function getStatusKucoin() {
    return axios.get(process.env.KUCOIN_API);
}

function getStatusDfx() {
    return axios.get(process.env.DFX_API);
}

async function sendUpdateNewsletterMail(mail, address, status) {

    try {

        fs.readFile(__dirname + '/templates/update.mjml', 'utf8', async function read(err, data) {

            if (err) {
                logger.error("Read template update error");
                logger.error(err.message);
                return;
            }

            let contentHtml = mjml2html(data.toString('utf8')).html;
            contentHtml = contentHtml.replace("{{mail}}", mail);
            contentHtml = contentHtml.replace("{{address}}", address);
            contentHtml = contentHtml.replace("{{status}}", status);

            await sendMail(mail, "Newsletter Data Updated", contentHtml);

        });

    } catch (err) {
        logger.error("SendUpdateNewsletterMail error for receiver: " + mail, err)
    }

}

function dfiForNewsletter(contentHtml, wallet, dfiInLm, balanceMasternodeToken, balanceMasternodeUtxo) {
    contentHtml = contentHtml.replace("{{dfiWallet}}", round(wallet.dfi));
    contentHtml = contentHtml.replace("{{dfiStaking}}", round(wallet.dfiInStaking));
    contentHtml = contentHtml.replace("{{dfiLm}}", round(dfiInLm));
    contentHtml = contentHtml.replace("{{dfiMasternodes}}", round(balanceMasternodeToken + balanceMasternodeUtxo));
    return contentHtml.replace("{{dfiTotal}}", round(dfiInLm + wallet.dfi + wallet.dfiInStaking + balanceMasternodeToken + balanceMasternodeUtxo));
}

function lmForNewsletter(contentHtml, wallet) {
    // Crypto
    contentHtml = contentHtml.replace("{{lmBtc}}", round(wallet.btcInBtcPool));
    contentHtml = contentHtml.replace("{{lmEth}}", round(wallet.ethInEthPool));
    contentHtml = contentHtml.replace("{{lmUsdt}}", round(wallet.usdtInUsdtPool));
    contentHtml = contentHtml.replace("{{lmUsdc}}", round(wallet.usdcInUsdcPool));
    contentHtml = contentHtml.replace("{{lmLtc}}", round(wallet.ltcInLtcPool));
    contentHtml = contentHtml.replace("{{lmBch}}", round(wallet.bchInBchPool));
    contentHtml = contentHtml.replace("{{lmDoge}}", round(wallet.dogeInDogePool));
    contentHtml = contentHtml.replace("{{dfiBch}}", round(wallet.dfiInBchPool));

    contentHtml = contentHtml.replace("{{dfiBtc}}", round(wallet.dfiInBtcPool));
    contentHtml = contentHtml.replace("{{dfiEth}}", round(wallet.dfiInEthPool));
    contentHtml = contentHtml.replace("{{dfiUsdt}}", round(wallet.dfiInUsdtPool));
    contentHtml = contentHtml.replace("{{dfiUsdc}}", round(wallet.dfiInUsdcPool));
    contentHtml = contentHtml.replace("{{dfiLtc}}", round(wallet.dfiInLtcPool));
    contentHtml = contentHtml.replace("{{dfiDoge}}", round(wallet.dfiInDogePool));

    // Stocks
    contentHtml = contentHtml.replace("{{lmDusd}}", round(wallet.usdInUsdPool));
    contentHtml = contentHtml.replace("{{lmSpy}}", round(wallet.spyInSpyPool));
    contentHtml = contentHtml.replace("{{lmQqq}}", round(wallet.qqqInQqqPool));
    contentHtml = contentHtml.replace("{{lmTsla}}", round(wallet.tslaInTslaPool));
    contentHtml = contentHtml.replace("{{lmAapl}}", round(wallet.aaplInAaplPool));
    contentHtml = contentHtml.replace("{{lmNvda}}", round(wallet.nvdaInNvdaPool));
    contentHtml = contentHtml.replace("{{lmGme}}", round(wallet.gmeInGmePool));
    contentHtml = contentHtml.replace("{{lmQqq}}", round(wallet.qqqInQqqPool));
    contentHtml = contentHtml.replace("{{lmCoin}}", round(wallet.coinInCoinPool));
    contentHtml = contentHtml.replace("{{lmAmzn}}", round(wallet.amznInAmznPool));
    contentHtml = contentHtml.replace("{{lmBaba}}", round(wallet.babaInBabaPool));
    contentHtml = contentHtml.replace("{{lmArkk}}", round(wallet.arkkInArkkPool));
    contentHtml = contentHtml.replace("{{lmPltr}}", round(wallet.pltrInPltrPool));
    contentHtml = contentHtml.replace("{{lmGoogl}}", round(wallet.googlInGooglPool));
    contentHtml = contentHtml.replace("{{lmTlt}}", round(wallet.tltInTltPool));
    contentHtml = contentHtml.replace("{{lmEem}}", round(wallet.eemInEemPool));
    contentHtml = contentHtml.replace("{{lmSlv}}", round(wallet.slvInSlvPool));
    contentHtml = contentHtml.replace("{{lmGld}}", round(wallet.gldInGldPool));
    contentHtml = contentHtml.replace("{{lmVnq}}", round(wallet.vnqInVnqPool));
    contentHtml = contentHtml.replace("{{lmUrth}}", round(wallet.urthInUrthPool));
    contentHtml = contentHtml.replace("{{lmPdbc}}", round(wallet.pdbcInPdbcPool));

    contentHtml = contentHtml.replace("{{dfiDusd}}", round(wallet.dfiInUsdPool));
    contentHtml = contentHtml.replace("{{dusdSpy}}", round(wallet.usdInSpyPool));
    contentHtml = contentHtml.replace("{{dusdQqq}}", round(wallet.usdInQqqPool));
    contentHtml = contentHtml.replace("{{dusdTsla}}", round(wallet.usdInTslaPool));
    contentHtml = contentHtml.replace("{{dusdAapl}}", round(wallet.usdInAaplPool));
    contentHtml = contentHtml.replace("{{dusdNvda}}", round(wallet.usdInNvdaPool));
    contentHtml = contentHtml.replace("{{dusdGme}}", round(wallet.usdInGmePool));
    contentHtml = contentHtml.replace("{{dusdQqq}}", round(wallet.usdInQqqPool));
    contentHtml = contentHtml.replace("{{dusdCoin}}", round(wallet.usdInCoinPool));
    contentHtml = contentHtml.replace("{{dusdAmzn}}", round(wallet.usdInAmznPool));
    contentHtml = contentHtml.replace("{{dusdBaba}}", round(wallet.usdInBabaPool));
    contentHtml = contentHtml.replace("{{dusdArkk}}", round(wallet.usdInArkkPool));
    contentHtml = contentHtml.replace("{{dusdPltr}}", round(wallet.usdInPltrPool));
    contentHtml = contentHtml.replace("{{dusdGoogl}}", round(wallet.usdInGooglPool));
    contentHtml = contentHtml.replace("{{dusdTlt}}", round(wallet.usdInTltPool));
    contentHtml = contentHtml.replace("{{dusdEem}}", round(wallet.usdInEemPool));
    contentHtml = contentHtml.replace("{{dusdSlv}}", round(wallet.usdInSlvPool));
    contentHtml = contentHtml.replace("{{dusdGld}}", round(wallet.usdInGldPool));
    contentHtml = contentHtml.replace("{{dusdVnq}}", round(wallet.usdInVnqPool));
    contentHtml = contentHtml.replace("{{dusdUrth}}", round(wallet.usdInUrthPool));
    contentHtml = contentHtml.replace("{{dusdPdbc}}", round(wallet.usdInPdbcPool));
    return contentHtml;
}

function walletForNewsletter(contentHtml, wallet) {
    // Crypto
    contentHtml = contentHtml.replace("{{walletBtc}}", round(wallet.btc));
    contentHtml = contentHtml.replace("{{walletEth}}", round(wallet.eth));
    contentHtml = contentHtml.replace("{{walletUsdt}}", round(wallet.usdt));
    contentHtml = contentHtml.replace("{{walletUsdc}}", round(wallet.usdc));
    contentHtml = contentHtml.replace("{{walletLtc}}", round(wallet.ltc));
    contentHtml = contentHtml.replace("{{walletBch}}", round(wallet.bch));
    contentHtml = contentHtml.replace("{{walletDoge}}", round(wallet.doge));
    // Stocks
    contentHtml = contentHtml.replace("{{walletDusd}}", round(wallet.usd));
    contentHtml = contentHtml.replace("{{walletSpy}}", round(wallet.spy));
    contentHtml = contentHtml.replace("{{walletQqq}}", round(wallet.qqq));
    contentHtml = contentHtml.replace("{{walletTsla}}", round(wallet.tsla));
    contentHtml = contentHtml.replace("{{walletAapl}}", round(wallet.aapl));
    contentHtml = contentHtml.replace("{{walletNvda}}", round(wallet.nvda));
    contentHtml = contentHtml.replace("{{walletGme}}", round(wallet.gme));
    contentHtml = contentHtml.replace("{{walletQqq}}", round(wallet.qqq));
    contentHtml = contentHtml.replace("{{walletCoin}}", round(wallet.coin));
    contentHtml = contentHtml.replace("{{walletAmzn}}", round(wallet.amzn));
    contentHtml = contentHtml.replace("{{walletBaba}}", round(wallet.baba));
    contentHtml = contentHtml.replace("{{walletArkk}}", round(wallet.arkk));
    contentHtml = contentHtml.replace("{{walletPltr}}", round(wallet.pltr));
    contentHtml = contentHtml.replace("{{walletGoogl}}", round(wallet.googl));
    contentHtml = contentHtml.replace("{{walletTlt}}", round(wallet.tlt));
    contentHtml = contentHtml.replace("{{walletEem}}", round(wallet.eem));
    contentHtml = contentHtml.replace("{{walletSlv}}", round(wallet.slv));
    contentHtml = contentHtml.replace("{{walletGld}}", round(wallet.gld));
    contentHtml = contentHtml.replace("{{walletVnq}}", round(wallet.vnq));
    contentHtml = contentHtml.replace("{{walletUrth}}", round(wallet.urth));
    contentHtml = contentHtml.replace("{{walletPdbc}}", round(wallet.pdbc));

    return contentHtml;
}

function accountInfoForNewsletter(contentHtml, user, price) {
    contentHtml = contentHtml.replace("{{account}}", user.key);
    contentHtml = contentHtml.replace("{{value}}", round(user.totalValue));
    contentHtml = contentHtml.replace("{{incomeDfi}}", round(user.totalValueIncomeDfi));
    contentHtml = contentHtml.replace("{{incomeUsd}}", round(user.totalValueIncomeUsd));
    return contentHtml.replace("{{dfiPrice}}", "" + round(price?.price.aggregated.amount));
}

async function sendNewsletterMail(user) {

    try {

        fs.readFile(__dirname + '/templates/newsletter.mjml', 'utf8', async function read(err, data) {

            if (err) {
                logger.error("Read template newsletter error");
                logger.error(err.message);
                return;
            }

            const price = await client.prices.get("DFI", "USD");
            const vaults = []

            for (let i = 0; i < user.addresses.length; i++) {
                const vaultsAddress = await client.address.listVault(user.addresses[i]);
                for (let j = 0; j < vaultsAddress.length; j++) {
                    vaults.push(vaultsAddress[j]);
                }

            }

            let balanceMasternodeUtxo = 0;
            let balanceMasternodeToken = 0;
            for (let i = 0; i < user.addressesMasternodes.length; i++) {
                balanceMasternodeUtxo += +await client.address.getBalance(user.addressesMasternodes[i]);
                balanceMasternodeToken += +(await client.address.listToken(user.addressesMasternodes[i])).filter(t => t.id === "0");
            }
            const wallet = user.wallet;
            const dfiInLm = wallet.dfiInBtcPool + wallet.dfiInEthPool + wallet.dfiInUsdtPool + wallet.dfiInUsdcPool
                + wallet.dfiInLtcPool + wallet.dfiInDogePool + wallet.dfiInBchPool;

            // transform mjml to html
            let contentHtml = mjml2html(data.toString()).html;

            // basic infos of account
            contentHtml = accountInfoForNewsletter(contentHtml, user, price);
            // wallet
            contentHtml = walletForNewsletter(contentHtml, wallet);
            // lm
            contentHtml = lmForNewsletter(contentHtml, wallet);
            // dfi
            contentHtml = dfiForNewsletter(contentHtml, wallet, dfiInLm, balanceMasternodeToken, balanceMasternodeUtxo);

            // Vaults
            const templateVault = fs.readFileSync(__dirname + '/templates/vault.mjml', {encoding: 'utf8', flag: 'r'});
            let vaultsHtml = mjml2html(templateVault).html;
            let vaultsHtmlResult = "";

            for (let i = 0; i < vaults.length; i++) {
                let contentHtmlVault = vaultsHtml;
                contentHtmlVault = contentHtmlVault.replace("{{vaultId}}", vaults[i].vaultId);
                const nameVault = "..." + vaults[i].vaultId.slice(vaults[i].vaultId.length - 5, vaults[i].vaultId.length)
                    + " - " + vaults[i].state + " - " + vaults[i].collateralRatio + " %/" + vaults[i].loanScheme.minColRatio + "%";
                contentHtmlVault = contentHtmlVault.replace("{{vault}}", nameVault);
                vaultsHtmlResult = vaultsHtmlResult + contentHtmlVault;
            }

            contentHtml = contentHtml.replace("{{vaults}}", vaultsHtmlResult);

            await sendMail(user.newsletter.email, "Newsletter", contentHtml);

        });

    } catch (err) {
        logger.error("sendNewsletterMail error for user: " + user.key, err)
    }

}

function round(number) {
    return number > 0 ? (Math.round(number * 100) / 100).toString() : 0;
}

async function sendMail(receiver, subject, content) {
    try {

        logger.info("Mail sending start  to " + receiver);

        let info = await mailer.sendMail({
            from: "DeFiChain-Income.com <defichain-income@topiet.de>",
            to: receiver,
            subject: subject,
            text: content,
            html: content,
        });

        logger.info("Mail sending to " + receiver + ", sent and finished: " + info.messageId);

    } catch (err) {
        logger.error("Send Mail error for receiver: " + receiver, err)
    }
}

async function checkNewsletterPayed(address) {
    const dateNow = new Date();
    const firstPage = await client.address.listTransaction(payingAddress, 100)
    const network = MainNet.name
    let foundSource = false;
    let foundTarget = false;
    let foundTargetPayed = false;
    for (const t of firstPage) {
        const txId = t.txid;
        const date = new Date(t.block.time * 1000);
        if (dateNow.getUTCMonth() === date.getUTCMonth()) {
            const outs = await client.transactions.getVouts(txId, 100);
            const ins = await client.transactions.getVins(txId, 100);
            // Source is correct
            for (const i of ins) {
                const addressDec = fromScriptHex.fromScriptHex(i.vout.script?.hex, network);
                if (addressDec && addressDec.address === address) {
                    foundSource = true;
                    break;
                }
            }
            // Target is correct and payed
            for (const o of outs) {
                const addressDec = fromScriptHex.fromScriptHex(o.script?.hex, network);
                if (addressDec && addressDec.address === payingAddress) {
                    foundTarget = true;
                    if (+o.value > 0.99) {
                        foundTargetPayed = true;
                        break;
                    }

                }
            }

        }
    }
    return foundSource && foundTarget && foundTargetPayed;
}

async function saveBTCPool(data) {

    const createdBTCPOOL = await PoolBTC.create(assignDataValue(Object.values(data)[0], new PoolBTC(), "5"));
    return createdBTCPOOL;
}

async function saveETHPool(data) {

    const createdETHPOOL = await PoolETH.create(assignDataValue(Object.values(data)[0], new PoolETH(), "4"));
    return createdETHPOOL;
}

async function saveLTCPool(data) {

    const createdLTCPOOL = await PoolLTC.create(assignDataValue(Object.values(data)[0], new PoolLTC(), "10"));
    return createdLTCPOOL;
}

async function saveUSDTPool(data) {

    const createdUSDTPool = await PoolUSDT.create(assignDataValue(Object.values(data)[0], new PoolUSDT(), "6"));
    return createdUSDTPool;
}

async function saveUSDCPool(data) {


    const createdUSDCPool = await PoolUSDC.create(assignDataValue(Object.values(data)[0], new PoolUSDC(), "14"));
    return createdUSDCPool;
}

async function saveBCHPool(data) {

    const createdBCHPool = await PoolBCH.create(assignDataValue(Object.values(data)[0], new PoolBCH(), "12"));
    return createdBCHPool;
}

async function saveDOGEPool(data) {

    const createdDOGEPool = await PoolDOGE.create(assignDataValue(Object.values(data)[0], new PoolDOGE(), "8"));
    return createdDOGEPool;
}

async function saveUSDPool(data) {

    const poolData = Object.values(data)[0];
    const pool = assignDataValue(poolData, new PoolUSD(), "17")

    pool.totalLiquidityUsd = poolData.totalLiquidityUsd;
    pool.rewardPct = poolData.rewardPct;
    pool.symbol = poolData.symbol;
    pool.reserveA = poolData.reserveA;
    pool.reserveB = poolData.reserveB;
    pool.commission = poolData.commission;
    pool.customRewards = poolData.customRewards;
    pool.totalLiquidityLpToken = poolData.totalLiquidity;
    pool.totalLiquidity = poolData.totalLiquidity;

    pool.priceA = pool.totalLiquidityUsd / 2 / pool.reserveA;
    pool.priceB = pool.totalLiquidityUsd / 2 / pool.reserveB;

    const createdUSDPool = await PoolUSD.create(pool);
    return createdUSDPool;
}

async function saveTSLAPool(data) {
    const poolData = Object.values(data)[0];
    const pool = assignDataValue(poolData, new PoolTSLA(), "18")

    pool.totalLiquidityUsd = poolData.totalLiquidityUsd;
    pool.rewardPct = poolData.rewardPct;
    pool.symbol = poolData.symbol;
    pool.reserveA = poolData.reserveA;
    pool.reserveB = poolData.reserveB;
    pool.commission = poolData.commission;
    pool.customRewards = poolData.customRewards;
    pool.totalLiquidityLpToken = poolData.totalLiquidity;
    pool.totalLiquidity = poolData.totalLiquidity;

    pool.priceA = pool.totalLiquidityUsd / 2 / pool.reserveA;
    pool.priceB = 1;

    const createdTSLAPool = await PoolTSLA.create(pool);
    return createdTSLAPool;
}

async function saveFarmingPool(dataPairs) {
    const pools = [];

    const poolPairs = Object.values(dataPairs);
    const usdPool = poolPairs.find(x => x.id === '17');
    const usdPrice = usdPool.totalLiquidityUsd / 2 / usdPool.reserveA

    poolPairs.forEach(p => {
        const pool = assignDataValue(p, {}, p.id);
        const poolFromPairs = p;
        pool.totalLiquidityUsd = poolFromPairs.totalLiquidityUsd;
        pool.rewardPct = poolFromPairs.rewardPct;
        pool.symbol = poolFromPairs.symbol;
        pool.reserveA = poolFromPairs.reserveA;
        pool.reserveB = poolFromPairs.reserveB;
        pool.commission = poolFromPairs.commission;
        pool.customRewards = poolFromPairs.customRewards;
        pool.totalLiquidityLpToken = poolFromPairs.totalLiquidity;
        pool.totalLiquidity = poolFromPairs.totalLiquidity;

        // Crypto Pool price
        if (+p.id < 17) {
            pool.priceA = pool.totalLiquidityUsd / 2 / pool.reserveA;
            pool.priceB = pool.totalLiquidityUsd / 2 / pool.reserveB;
        }

        // USD Pool price
        if (+p.id === 17) {
            pool.priceA = usdPrice;
            pool.priceB = pool.totalLiquidityUsd / 2 / pool.reserveB;
        }

        // Other Stocks
        if (+p.id > 17) {
            pool.priceA = pool.totalLiquidityUsd / 2 / pool.reserveA;
            pool.priceB = usdPrice;
        }

        pools.push(pool);
    })
    const createdFarmingPool = await PoolFarming.create({pools, tvl: 0, date: new Date()});
    return createdFarmingPool;
}

async function saveStats(data, dataOcean) {

    const stats = new Stats();
    assignDataValueStats(data, stats);
    assignDataValueStatsOcean(dataOcean, stats)

    const createdStats = await Stats.create(stats);
    return createdStats;
}

async function computeCorrelation(data) {

    const millisecondsBefore = new Date().getTime();

    const tillDate = new Date();
    const fromDate = new Date(tillDate - (24 * 60 * 60 * 1000) * (data ? data : 365));

    const poollist = await PoolFarming.find({
        date: {'$gte': fromDate, '$lte': tillDate},
        "$expr": {
            "$and": [{
                "$eq": [
                    {"$minute": "$date"}, 0
                ]
            }]
        }
    }).sort({date: 1}).lean();

    const btc = []
    const eth = []
    const ltc = []
    const doge = []
    const bch = []
    const usdt = []
    const usdc = []
    const usd = []
    const tsla = []

    const dfiBtc = []
    const dfiEth = []
    const dfiLtc = []
    const dfiDoge = []
    const dfiBch = []
    const dfiUsdc = []
    const dfiUsd = []
    const dfiTsla = []

    poollist.forEach(p => {
        p.pools.forEach(pool => {
            if (!pool.priceB || !pool.priceA) {
                return;
            }
            if (pool.pair === "BTC-DFI") {
                btc.push(pool.priceA);
                dfiBtc.push(pool.priceB);
            } else if (pool.pair === "ETH-DFI") {
                eth.push(pool.priceA);
                dfiEth.push(pool.priceB);
            } else if (pool.pair === "LTC-DFI") {
                ltc.push(pool.priceA);
                dfiLtc.push(pool.priceB);
            } else if (pool.pair === "BCH-DFI") {
                bch.push(pool.priceA);
                dfiBch.push(pool.priceB);
            } else if (pool.pair === "DOGE-DFI") {
                doge.push(pool.priceA);
                dfiDoge.push(pool.priceB);
            } else if (pool.pair === "USDT-DFI") {
                usdt.push(pool.priceA);
            } else if (pool.pair === "USDC-DFI") {
                usdc.push(pool.priceA);
                dfiUsdc.push(pool.priceB);
            } else if (pool.pair === "DUSD-DFI") {
                usd.push(pool.priceA);
                dfiUsd.push(pool.priceB);
            } else if (pool.pair === "TSLA-DFI") {
                tsla.push(pool.priceA);
                dfiTsla.push(pool.priceB);
            }
        });
    });

    const corUsd = (usd.length > 0 && dfiUsd > 0) ? CorrelationComputing(usd, dfiUsd) : -1;
    const corTsla = (tsla.length > 0 && dfiTsla > 0) ? CorrelationComputing(tsla, dfiTsla) : -1;

    const correlation = {
        btcPool: (Math.round(CorrelationComputing(btc, dfiBtc) * 1000) / 1000).toFixed(3),
        ethPool: (Math.round(CorrelationComputing(eth, dfiEth) * 1000) / 1000).toFixed(3),
        ltcPool: (Math.round(CorrelationComputing(ltc, dfiLtc) * 1000) / 1000).toFixed(3),
        bchPool: (Math.round(CorrelationComputing(bch, dfiBch) * 1000) / 1000).toFixed(3),
        dogePool: (Math.round(CorrelationComputing(doge, dfiDoge) * 1000) / 1000).toFixed(3),
        usdtPool: (Math.round(CorrelationComputing(usdt, dfiBtc) * 1000) / 1000).toFixed(3),
        usdcPool: (Math.round(CorrelationComputing(usdc, dfiUsdc) * 1000) / 1000).toFixed(3),
        usdPool: isNaN(corUsd) ? 999 : (Math.round(corUsd * 1000) / 1000).toFixed(3),
        tslaPool: isNaN(corTsla) ? 999 : (Math.round(corTsla * 1000) / 1000).toFixed(3),

        btcPricesDex: btc,
        ethPricesDex: eth,
        ltcPricesDex: ltc,
        dogePricesDex: doge,
        bchPricesDex: bch,
        usdtPricesDex: usdt,
        usdcPricesDex: usdc,
        usdPricesDex: usd,
        tslaPricesDex: tsla,
        dfiPricesDex: dfiBtc

    };

    const millisecondsAfter = new Date().getTime();
    const msTime = millisecondsAfter - millisecondsBefore;
    logger.info("Get Correlation for " + data + " days in " + msTime + " ms.");

    return correlation;

}

function assignDataValue(data, object, id) {

    object.date = new Date();
    object.poolId = id;
    object.poolPairId = data.poolPairId;
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

    object.rewardPct = data.rewardPct;
    object.commission = data.commission;
    object.symbol = data.symbol;
    object.volumeA30 = data.volumeA30;
    object.volumeB30 = data.volumeB30;
    return object;

}

function assignDataValueStats(data, object) {
    object.tokens = Object.assign({}, data.tokens);
    return object;
}

function assignDataValueStatsOcean(data, object) {

    object.loan = Object.assign({}, data.data.loan);
    object.net = Object.assign({}, data.data.net);
    object.count = Object.assign({}, data.data.count);
    object.burned = Object.assign({}, data.data.burned);
    object.tvl = Object.assign({}, data.data.tvl);
    object.price = Object.assign({}, data.data.price);
    object.masternodes = Object.assign({}, data.data.masternodes);
    object.blockHeight = data.data.count.blocks;
    object.difficulty = data.data.blockchain.difficulty;
    object.rewards.total = data.data.emission.total;
    object.rewards.community = data.data.emission.community;
    object.rewards.anchorReward = data.data.emission.anchor;
    object.rewards.liquidityPool = data.data.emission.dex;
    object.rewards.minter = data.data.emission.masternode;
    object.rewards.burned = data.data.emission.burned;
    return object;
}

const app = express();


if (process.env.JOB_SCHEDULER_ON === "on") {
    schedule.scheduleJob(process.env.JOB_SCHEDULER_TURNUS, function () {
        const millisecondsBefore = new Date().getTime();
        logger.info("===============Pools Job started " + new Date() + " =================");

        Promise.all([getPool("5"), getPool("4"), getPool("6"), getPool("10"), getPool("8"), getPool("12"), getPool("14"), getPool("17"), getPool("18"), getPoolPairs()])
            .then(function (results) {
                const btc = results[0];
                saveBTCPool(btc.data)
                    .catch(function (error) {
                        // handle error
                        logger.error("saveBTCPool", error);
                    });

                const eth = results[1];
                saveETHPool(eth.data)
                    .catch(function (error) {
                        // handle error
                        logger.error("saveETHPool", error);
                    });
                const usdt = results[2];
                saveUSDTPool(usdt.data)
                    .catch(function (error) {
                        // handle error
                        logger.error("saveUSDTPool", error);
                    });
                const ltc = results[3];
                saveLTCPool(ltc.data)
                    .catch(function (error) {
                        // handle error
                        logger.error("saveLTCPool", error);
                    });
                const doge = results[4];
                saveDOGEPool(doge.data)
                    .catch(function (error) {
                        // handle error
                        logger.error("saveDOGEPool", error);
                    });
                const bch = results[5];
                saveBCHPool(bch.data)
                    .catch(function (error) {
                        // handle error
                        logger.error("saveBCHPool", error);
                    });
                const usdc = results[6];
                saveUSDCPool(usdc.data)
                    .catch(function (error) {
                        // handle error
                        logger.error("saveUSDCPool", error);
                    });
                const usd = results[7];
                saveUSDPool(usd.data)
                    .catch(function (error) {
                        // handle error
                        logger.error("saveUSDPool", error);
                    });
                const tsla = results[8];
                saveTSLAPool(tsla.data)
                    .catch(function (error) {
                        // handle error
                        logger.error("saveTSLAPool", error);
                    });
                const pairs = results[9];
                saveFarmingPool(pairs.data).catch(function (error) {
                    // handle error
                    logger.error("saveFarmingPool", error);
                });

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                logger.info("=============Pools Job executed time: " + new Date() + " in " + msTime + " ms.============");

            }).catch(function (error) {
            // handle error
            if (error.response) {
                // Request made and server responded
                logger.error("==================== ERROR PoolJob in Call to API BEGIN ====================");
                logger.error("PoolJob", error.response.data);
                logger.error("PoolJob", error.response.status);
                logger.error("PoolJob", error.response.statusText);
                logger.error("==================== ERROR PoolJob in Call to API END ====================");
            } else if (error.request) {
                // The request was made but no response was received
                logger.error("PoolJob", error.request);
            } else {
                // Something happened in setting up the request that triggered an Error
                logger.error('PoolJob', error.message);
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
        logger.info("============Stats Job started " + new Date() + " =============");
        axios.all([
            getStats(),
            getStatsOcean()
        ])
            .then(axios.spread((response, response2) => {
                saveStats(response.data, response2.data)
                    .catch(function (error) {
                        // handle error
                        logger.error("Stats Job", error);
                    });

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                logger.info("Stats Job executed time: " + new Date() + " in " + msTime + " ms.");
            }))
            .catch(function (error) {
                // handle error
                if (error.response) {
                    // Request made and server responded
                    logger.error("==================== ERROR stats in Call to API BEGIN ====================");
                    logger.error("Stats Job", error.response.data);
                    logger.error("Stats Job", error.response.status);
                    logger.error("Stats Job", error.response.statusText);
                    logger.error("==================== ERROR stats in Call to API END ====================");
                } else if (error.request) {
                    // The request was made but no response was received
                    logger.error("Stats Job", error.request);
                } else {
                    // Something happened in setting up the request that triggered an Error
                    logger.error("Stats Job", error.message);
                }

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                logger.info("Stats Job executed time: " + new Date() + " in " + msTime + " ms.");
            });
    });
}

if (process.env.JOB_SCHEDULER_ON_HISTORY === "on") {
    schedule.scheduleJob(process.env.JOB_SCHEDULER_TURNUS_HISTORY, async function () {
        const millisecondsBefore = new Date().getTime();
        logger.info("===========History Job started: " + new Date() + " ================");

        const users = await User.find().lean();

        logger.info("===========History Job started users " + users.length + " ================");

        let updated = 0;
        let created = 0;

        for (const u of users) {

            try {

                if (u.totalValue > 0 || u.totalValueIncomeDfi > 0 || u.totalValueIncomeUsd > 0) {

                    // Load User and History
                    const userHistoryLoaded = await findHistoryByKey(u.key);

                    // Update History
                    if (userHistoryLoaded && u) {
                        const item = {
                            date: new Date(),
                            totalValue: u.totalValue ? u.totalValue : 0,
                            totalValueIncomeDfi: u.totalValueIncomeDfi ? u.totalValueIncomeDfi : 0,
                            totalValueIncomeUsd: u.totalValueIncomeUsd ? u.totalValueIncomeUsd : 0
                        };
                        userHistoryLoaded.values.push(item);
                        await userHistoryLoaded.save();
                        updated = updated + 1;
                        continue;
                    }

                    // Save New history
                    const createdUserHistory = await UserHistory.create({
                        key: u.key,
                        values: [{
                            date: new Date(),
                            totalValue: u.totalValue ? u.totalValue : 0,
                            totalValueIncomeDfi: u.totalValueIncomeDfi ? u.totalValueIncomeDfi : 0,
                            totalValueIncomeUsd: u.totalValueIncomeUsd ? u.totalValueIncomeUsd : 0
                        }]
                    });

                    created = created + 1;

                }

            } catch (e) {
                // Something happened in setting up the request that triggered an Error
                logger.error("============ History Job error with key " + u.key, e.message);
            }

        }

        const millisecondsAfter = new Date().getTime();
        const msTime = millisecondsAfter - millisecondsBefore;

        logger.info("============History Job executed created " + created + " and updated " + updated + " =============");
        logger.info("============History Job executed time: " + new Date() + " in " + msTime + " ms.=============");

    });
}

if (process.env.JOB_SCHEDULER_ON_NEWSLETTER === "on") {
    schedule.scheduleJob(process.env.JOB_SCHEDULER_TURNUS_NEWSLETTER, async function () {
        const millisecondsBefore = new Date().getTime();
        logger.info("===========Newsletter Job: started: " + new Date() + " ================");

        const users = await User.find({ newsletter: { $ne: null } }).lean();

        logger.info("===========Newsletter Job: Start Sending Newsletter for subscribers: " + users.length + " ================");

        let mail = 0;
        let address = 0;

        for (let i = 0; i < users.length; i++) {

            const u = users[i];

            try {

                if (u.newsletter.email && u.newsletter.email.length > 0) {
                    mail++;
                    logger.info("===========Newsletter start for user:  " + u.key + " ================");
                    await sendNewsletterMail(u)
                    logger.info("============ Newsletter finished for user: " + u.key + " ================");
                }
                if (u.newsletter.payingAddress && u.newsletter.payingAddress.length > 0) {
                    address++;
                }


            } catch (e) {
                logger.error("============ Newsletter Job error for user with key " + u.key, e.message);
            }
        }

        logger.info("===========Newsletter Subscriber " + users.length + " ================");
        logger.info("===========Newsletter Subscriber Mail " + mail + " ================");
        logger.info("===========Newsletter Subscriber Addresses " + address + " ================");

        const millisecondsAfter = new Date().getTime();
        const msTime = millisecondsAfter - millisecondsBefore;

        logger.info("============Newsletter Job executed time: " + new Date() + " in " + msTime + " ms.=============");

    });
}

const corsOptions = {
    origin: '*',
    credentials: true // <-- REQUIRED backend setting
};

async function startServer() {

    const server = new ApolloServer({
        typeDefs,
        resolvers,
        formatError: (err) => {
            // When debug return only message
            if (process.env.DEBUG === "off")
                return {message: err.message};

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

    await server.start();

    server.applyMiddleware({app, cors: corsOptions});

}

startServer();

app.listen({port: 4000}, () => {
        logger.info(`🚀 Server ready at http://localhost:4000/graphql`)
        logger.info("JOB Pools " + process.env.JOB_SCHEDULER_ON  + " Cron " + process.env.JOB_SCHEDULER_TURNUS)
        logger.info("JOB Stats " + process.env.JOB_SCHEDULER_ON_STATS + " Cron " + process.env.JOB_SCHEDULER_TURNUS_STATS)
        logger.info("JOB History " + process.env.JOB_SCHEDULER_ON_HISTORY + " Cron " + process.env.JOB_SCHEDULER_TURNUS_HISTORY)
        logger.info("JOB Newsletter " + process.env.JOB_SCHEDULER_ON_NEWSLETTER + " Cron " + process.env.JOB_SCHEDULER_TURNUS_NEWSLETTER)
        logger.info("DEBUG " + process.env.DEBUG)

    }
);


