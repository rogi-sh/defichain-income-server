const {ApolloServer, gql} = require("apollo-server-express");
const express = require('express');
const {GraphQLScalarType, subscribe} = require("graphql");
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

require('events').EventEmitter.setMaxListeners(300);

const client = new WhaleApiClient({
    url: 'https://ocean.defichain.com',
    timeout: 60000,
    version: 'v0',
    network: 'mainnet'
})

const mailer = nodemailer.createTransport({
    pool: true,
    maxConnections: 10,
    host: process.env.MAIL_SERVER,
    port: 587,
    secure: false, // true for 465, false for other ports
    auth: {
        user: process.env.MAIL_USER,
        pass: process.env.MAIL_PASSWORD,
    }});

mailer.verify(function(error, success) {
    if (error) {
        logger.error(error);
    } else {
        logger.info('Mail Server is ready to take our messages');
    }
});

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
const {PriceFeedTimeInterval} = require("@defichain/whale-api-client/dist/api/prices");

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

    msftusd: Number,
    voousd: Number,
    nflxusd: Number,
    fbusd: Number,

    disusd: Number,
    mchiusd: Number,
    mstrusd: Number,
    intcusd: Number,

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
    usdInEemPool: Number,

    // MSFT Pool
    msftInMsftPool: Number,
    msft: Number,
    usdInMsftPool: Number,

    // VOO Pool
    vooInVooPool: Number,
    voo: Number,
    usdInVooPool: Number,

    // FB Pool
    fbInFbPool: Number,
    fb: Number,
    usdInFbPool: Number,

    // NFLX Pool
    nflxInNflxPool: Number,
    nflx: Number,
    usdInNflxPool: Number,

    // DIS Pool
    disInDisPool: Number,
    dis: Number,
    usdInDisPool: Number,

    // MCHI Pool
    mchiInMchiPool: Number,
    mchi: Number,
    usdInMchiPool: Number,

    // MSTR Pool
    mstrInMstrPool: Number,
    mstr: Number,
    usdInMstrPool: Number,

    // INTC Pool
    intcInIntcPool: Number,
    intc: Number,
    usdInIntcPool: Number
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
    aktiv: Boolean
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
    commission: Number,
    volume24h: Number,
    volume30d: Number
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
        msftusd: Float
        voousd: Float
        fbusd: Float
        nflxusd: Float
        disusd: Float
        mchiusd: Float
        mstrusd: Float
        intcusd: Float

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
        
        # MSFT Pool
        msftInMsftPool: Float
        msft: Float
        usdInMsftPool: Float

        # VOO Pool
        vooInVooPool: Float
        voo: Float
        usdInVooPool: Float

        # FB Pool
        fbInFbPool: Float
        fb: Float
        usdInFbPool: Float

        # NFLX Pool
        nflxInNflxPool: Float
        nflx: Float
        usdInNflxPool: Float
        
        # DIS Pool
        disInDisPool: Float
        dis: Float
        usdInDisPool: Float

        # MCHI Pool
        mchiInMchiPool: Float
        mchi: Float
        usdInMchiPool: Float

        # MSTR Pool
        mstrInMstrPool: Float
        mstr: Float
        usdInMstrPool: Float

        # INTC Pool
        intcInIntcPool: Float
        intc: Float
        usdInIntcPool: Float
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
        volume24h: Float
        volume30d: Float
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
        aktiv: Boolean
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
        _id: ID
        
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
    
     type PriceHistory {
        price: Float,
        dateTime: Date
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
        getOracleHistory(token: String!, date: Date): [PriceHistory]
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
        msftusd: Float
        voousd: Float
        fbusd: Float
        nflxusd: Float
        
        disusd: Float
        mchiusd: Float
        mstrusd: Float
        intcusd: Float
        
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
        
         # MSFT Pool
        msftInMsftPool: Float
        msft: Float
        usdInMsftPool: Float

        # VOO Pool
        vooInVooPool: Float
        voo: Float
        usdInVooPool: Float

        # FB Pool
        fbInFbPool: Float
        fb: Float
        usdInFbPool: Float

        # NFLX Pool
        nflxInNflxPool: Float
        nflx: Float
        usdInNflxPool: Float   
        
        # DIS Pool
        disInDisPool: Float
        dis: Float
        usdInDisPool: Float

        # MCHI Pool
        mchiInMchiPool: Float
        mchi: Float
        usdInMchiPool: Float

        # MSTR Pool
        mstrInMstrPool: Float
        mstr: Float
        usdInMstrPool: Float

        # INTC Pool
        intcInIntcPool: Float
        intc: Float
        usdInIntcPool: Float    
    }
    
    input AddressV2Input {
        addressId: String
        masternode: Boolean
        freezer: Float
        name: String
        aktiv: Boolean
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
    
    input DeleteUserHistoryInput {
        key: String!
        items: [String]
   
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
        deleteUserHistory(user: DeleteUserHistoryInput): UserHistory
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


function totalValuesGreaterZero(user) {
    return user.totalValue > 0 && user.totalValueIncomeDfi > 0 && user.totalValueIncomeUsd > 0 &&  user.totalValue < 30000000;
}

async function loadExchangeInfos() {
    let bittrexStatus;
    let bittrexNotice;
    let kucoinStatusDeposit;
    let kucoinStatusWithdraw;
    let dfxBuy;
    let dfxSell;
    let dfxStaking

    logger.info("loadExchangeStatus: Start load exchange Status");

    await axios.all([
        getStatusBittrex(), getStatusKucoin(), getStatusDfx()
    ])
        .then(axios.spread((response, response2, response3) => {

            logger.info("loadExchangeStatus: Responses arrived");
            bittrexStatus = response.data.status;
            bittrexNotice = response.data.notice;
            kucoinStatusDeposit = response2.data.data.isDepositEnabled;
            kucoinStatusWithdraw = response2.data.data.isWithdrawEnabled;
            dfxBuy = response3.data.buy;
            dfxSell = response3.data.sell;
            dfxStaking = response3.data.staking;

        }))
        .catch(function (error) {
            logger.error("loadExchangeStatus err = " + JSON.stringify(error));
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
                logger.error("getExchangeStatus" + error.request);
            } else {
                // Something happened in setting up the request that triggered an Error
                logger.error('getExchangeStatus' + error.message);
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
    return exchangeStatus;
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
        getOracleHistory: async (obj, {token, date}, {auth}) => {
            try {
                const millisecondsBefore = new Date().getTime();

                let prices = [];

                let response = await client.prices.getFeedWithInterval(token, 'USD', PriceFeedTimeInterval.ONE_HOUR, 1000);

                for (const item of response) {
                    prices.push({dateTime: new Date(item.block.time * 1000), price: item.aggregated.amount});
                }

                while (response.hasNext) {
                    response = await client.paginate(response)
                    for (const item of response) {
                        prices.push({dateTime: new Date(item.block.time * 1000), price: item.aggregated.amount});
                    }
                }
                if (date) {
                    prices = prices.filter(a => {
                        return (a.dateTime >= date);
                    });
                }
                prices.reverse();

                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;
                logger.info("getOracleHistory " + new Date() + " called took " + msTime + " ms.");

                return prices;
            } catch (e) {
                logger.error("getOracleHistory", e);
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
                const exchangeStatus = await loadExchangeInfos();

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
                    farming = farming.filter(f => f.date.getUTCMinutes() === 0);
                } else if (diffDays >= 10 && diffDays < 30) {
                    farming = farming.filter(f => f.date.getUTCHours() === 0 || f.date.getUTCHours() === 6 ||
                        f.date.getUTCHours() === 12 || f.date.getHours() === 18);
                } else if (diffDays >= 30) {
                    farming = farming.filter(f => (f.date.getUTCHours() === 3 || f.date.getUTCHours() === 15) && f.date.getUTCMinutes() === 0);
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

                if (totalValuesGreaterZero(user)) {
                    logger.info("Update User total values ");
                    userLoaded.totalValue = user.totalValue;
                    userLoaded.totalValueIncomeDfi = user.totalValueIncomeDfi;
                    userLoaded.totalValueIncomeUsd = user.totalValueIncomeUsd;
                }

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
                const payed = user.payingAddress ? await checkNewsletterPayed(user.payingAddress) : false;
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

                if (user.email && user.email.length > 0) {
                    logger.info("Start Mail sending for update Newsletter to " + user.email);
                    await sendUpdateNewsletterMail(user.email, user.payingAddress, status)
                }

                //TEST NEWSLETTER
                //const stats = await client.stats.get();
                //const price = await client.prices.get("DFI", "USD");
                //const pools = await client.poolpairs.list(100);
                //const prices = await client.prices.list(1000);
                //const cake = await getCakeApy();
                //const exchanges = await loadExchangeInfos();
                //const result = await sendNewsletterMail(saved, stats, price, pools, prices, cake, exchanges);

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
        },
        deleteUserHistory: async (obj, {user}, {auth}) => {
            try {

                const millisecondsBefore = new Date().getTime();

                const userHistory = await findHistoryByKey(user.key);
                if (!userHistory) {
                    return null;
                }

                // delete false items
                for (const userHistoryElement of user.items) {
                    const ids = userHistory.values.map(function(e) { return e._id.toString() });
                    const index = ids.indexOf(userHistoryElement);
                    if (index > -1) {
                        userHistory.values.splice(index, 1);
                    }
                }

                const saved = await userHistory.save();


                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;

                logger.info("Delete User history called took " + msTime + " ms.");
                return saved;

            } catch (e) {
                logger.error("Delete User history", e);
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

function getCakeApy() {
    return axios.get(process.env.CAKE_API);
}

function getStats() {
    return axios.get(process.env.STATS_API);
}

function getStatsOcean() {
    return axios.get(process.env.STATS_OCEAN_API);
}

function getOceanPoolPairs() {
    return axios.get(process.env.POOL_PAIRS_OCEAN_API);
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

async function sendReportNewsletterMail(mail, subscriber, addresses, payed) {

    try {

        fs.readFile(__dirname + '/templates/report.mjml', 'utf8', async function read(err, data) {

            if (err) {
                logger.error("Read template report error");
                logger.error(err.message);
                return;
            }

            let contentHtml = mjml2html(data.toString('utf8')).html;
            contentHtml = contentHtml.replace("{{subscriber}}", subscriber);
            contentHtml = contentHtml.replace("{{addresses}}", addresses);
            contentHtml = contentHtml.replace("{{payed}}", payed);

            await sendMail(mail, "Newsletter Report", contentHtml);

        });

    } catch (err) {
        logger.error("sendReportNewsletterMail error for receiver: " + mail, err)
    }

}

async function sendNewsletterReminderMail(mail) {

    try {

        fs.readFile(__dirname + '/templates/payReminder.mjml', 'utf8', async function read(err, data) {

            if (err) {
                logger.error("Read template reminder error");
                logger.error(err.message);
                return;
            }

            let contentHtml = mjml2html(data.toString('utf8')).html;
            contentHtml = contentHtml.replace("{{payReminder}}", "If you would like to receive the Defichain-Income newsletter, please add your paying address and send the 1 DFI fee to the payment address for this month. ");

            await sendMail(mail, "Defi-Income Newsletter Reminder", contentHtml);

        });

    } catch (err) {
        logger.error("sendReminderNewsletterMail error for receiver: " + mail, err)
    }

}

function dfiForNewsletter(contentHtml, wallet, dfiInLm, balanceMasternodeToken, balanceMasternodeUtxo) {
    contentHtml = contentHtml.replace("{{dfiWallet}}", round(wallet.dfi));
    contentHtml = contentHtml.replace("{{dfiStaking}}", round(wallet.dfiInStaking));
    contentHtml = contentHtml.replace("{{dfiLm}}", round(dfiInLm));
    contentHtml = contentHtml.replace("{{dfiMasternodes}}", round(balanceMasternodeToken + balanceMasternodeUtxo));
    return contentHtml.replace("{{dfiTotal}}", round(dfiInLm + wallet.dfi + wallet.dfiInStaking + balanceMasternodeToken + balanceMasternodeUtxo));
}

function statisticsForNewsletter(contentHtml, stats, pools, prices) {

    // tvl
    contentHtml = contentHtml.replace("{{tvl.total}}", round(stats.tvl.total));
    contentHtml = contentHtml.replace("{{tvl.masternodes}}", round(stats.tvl.masternodes));
    contentHtml = contentHtml.replace("{{tvl.vaults}}", round(stats.tvl.loan));
    contentHtml = contentHtml.replace("{{tvl.dex}}", round(stats.tvl.dex));

    // pools
    const template = fs.readFileSync(__dirname + '/templates/pool.mjml', {encoding: 'utf8', flag: 'r'});
    let cryptoHtml = template;
    let cryptoHtmlResult = "";
    let index = 1;

    pools = pools.sort((a, b) => b.totalLiquidity.usd - a.totalLiquidity.usd);
    for (let i = 0; i < pools.length; i++) {
        const pool = pools[i];
        const price = pool.id === "17" ? 1: prices.find(p => p.id === (pool.tokenA.symbol + "-USD")).price.aggregated.amount;
        cryptoHtmlResult = cryptoHtmlResult + replacePoolStatisticsItem(cryptoHtml, pool, index, price);
        index++;
    }

    contentHtml = contentHtml.replace("{{pools}}", cryptoHtmlResult);


    return contentHtml;
}

function statisticsForStaking(contentHtml, stats, cake) {

    // staking
    const masternodeCount5Freezer = stats.masternodes.locked.find(p => p.weeks === 260).count;
    const masternodeCount10Freezer = stats.masternodes.locked.find(p => p.weeks === 520).count;
    const masternodeCount0Freezer = stats.masternodes.locked.find(p => p.weeks === 0).count;
    const masternodeCount = masternodeCount5Freezer + masternodeCount10Freezer + masternodeCount0Freezer;
    const masternodeCountForAprCalc = masternodeCount5Freezer * 1.5 + masternodeCount10Freezer * 2
        + masternodeCount0Freezer;
    const stakingAprMN = 60 / 30 * stats.emission.masternode / masternodeCountForAprCalc * 525600 / 20000 * 100;
    const stakingApyMn = 100 * (Math.pow(1 + (stakingAprMN / 100 / 52), 52) - 1);
    const stakingApy5Mn = 100 * (Math.pow(1 + (stakingAprMN * 1.5 / 100 / 52), 52) - 1);
    const stakingApy10Mn = 100 * (Math.pow(1 + (stakingAprMN * 2 / 100 / 52), 52) - 1);

    const cakeApy = +cake.data.staking.find(s => s.id === 'DFI').apy * 100;
    const cakeApr = 100 * 730 * (Math.pow(1 + cakeApy / 100, 1 / 730) - 1);

    // numbers
    contentHtml = contentHtml.replace("{{totalMN}}", masternodeCount);
    contentHtml = contentHtml.replace("{{normalMN}}", masternodeCount0Freezer);
    contentHtml = contentHtml.replace("{{5YMN}}", masternodeCount5Freezer);
    contentHtml = contentHtml.replace("{{10YMN}}", masternodeCount10Freezer);

    // staking masternode
    contentHtml = contentHtml.replace("{{aprMN}}", round2(stakingAprMN));
    contentHtml = contentHtml.replace("{{apyMN}}", round2(stakingApyMn));
    contentHtml = contentHtml.replace("{{aprMN5}}", round2(stakingAprMN * 1.5));
    contentHtml = contentHtml.replace("{{apyMN5}}", round2(stakingApy5Mn));
    contentHtml = contentHtml.replace("{{aprMN10}}", round2(stakingAprMN * 2));
    contentHtml = contentHtml.replace("{{apyMN10}}", round2(stakingApy10Mn));

    // staking cake
    contentHtml = contentHtml.replace("{{apyMNCake}}", round2(cakeApy));
    contentHtml = contentHtml.replace("{{aprMNCake}}", round2(cakeApr));

    return contentHtml;
}

function statisticsForExchnges(contentHtml, exchanges) {

    // dfx
    contentHtml = contentHtml.replace("{{dfxDeposit}}", exchanges.dfxBuy);
    contentHtml = contentHtml.replace("{{dfxWithdraw}}", exchanges.dfxSell);
    contentHtml = contentHtml.replace("{{dfxStaking}}", exchanges.dfxStaking);
    contentHtml = contentHtml.replace("{{dfxColor}}", exchanges.dfxBuy === 'ONLINE' ? '25e712': 'ff6666');

    // kucoin
    contentHtml = contentHtml.replace("{{kucoinDeposit}}", exchanges.kucoinStatusDeposit ? 'ONLINE' : 'OFFLINE');
    contentHtml = contentHtml.replace("{{kucoinWithdraw}}", exchanges.kucoinStatusWithdraw ? 'ONLINE' : 'OFFLINE');
    contentHtml = contentHtml.replace("{{kucoinColor}}", exchanges.kucoinStatusDeposit ? '25e712': 'ff6666');

    // bittrex
    contentHtml = contentHtml.replace("{{bittrexStatus}}", exchanges.bittrexStatus);
    contentHtml = contentHtml.replace("{{bittrexColor}}", exchanges.bittrexStatus === 'ONLINE' ? '25e712': 'ff6666');

    return contentHtml;
}

function replacePoolItem(contentHtmlCrypto, wallet, cryptoInPool, dfiInPool, index, crypto, dfi) {
    contentHtmlCrypto = contentHtmlCrypto.replace("{{lm}}", round(cryptoInPool));
    contentHtmlCrypto = contentHtmlCrypto.replace("{{dfi}}", round(dfiInPool));
    contentHtmlCrypto = contentHtmlCrypto.replace("{{wallet}}", round(wallet));
    contentHtmlCrypto = contentHtmlCrypto.replaceAll("{{walletTitle}}", crypto);
    contentHtmlCrypto = contentHtmlCrypto.replaceAll("{{dfiName}}", dfi);
    contentHtmlCrypto = contentHtmlCrypto.replace("{{color}}", index % 2 === 1 ? "e7e7e7" : 'fff');
    return contentHtmlCrypto;
}

function replacePoolStatisticsItem(contentHtmlPool, pool, index, price) {
    contentHtmlPool = contentHtmlPool.replace("{{pool.tvl}}", round2(pool.totalLiquidity.usd));
    contentHtmlPool = contentHtmlPool.replace("{{pool.name}}", pool.symbol);
    contentHtmlPool = contentHtmlPool.replace("{{pool.apr}}", round2(pool.apr.total * 100));
    const apy = 100 * (Math.pow(1 + (pool.apr.total * 100 / 100 / 52), 52) - 1);
    contentHtmlPool = contentHtmlPool.replace("{{pool.apy}}", round2(apy));
    contentHtmlPool = contentHtmlPool.replace("{{pool.t1.name}}", pool.tokenA.symbol);
    contentHtmlPool = contentHtmlPool.replace("{{pool.t2.name}}", pool.tokenB.symbol);
    contentHtmlPool = contentHtmlPool.replace("{{pool.t1.reserve}}", round2(pool.tokenA.reserve));
    contentHtmlPool = contentHtmlPool.replace("{{pool.t2.reserve}}", round2(pool.tokenB.reserve));
    const priceDex = pool.totalLiquidity.usd / pool.tokenA.reserve / 2;
    const premium = (priceDex / price * 100) - 100;
    contentHtmlPool = contentHtmlPool.replace("{{pool.cex}}", round2(price));
    contentHtmlPool = contentHtmlPool.replace("{{pool.dex}}", round2(priceDex));
    contentHtmlPool = contentHtmlPool.replace("{{pool.premium}}", roundNeg(premium));
    contentHtmlPool = contentHtmlPool.replace("{{color}}", index % 2 === 1 ? "e7e7e7" : 'fff');

    return contentHtmlPool;
}

function crypto(contentHtml, wallet) {

    const template = fs.readFileSync(__dirname + '/templates/wallet.mjml', {encoding: 'utf8', flag: 'r'});
    let cryptoHtml = template;
    let cryptoHtmlResult = "";
    let index = 1;

    let contentHtmlCrypto = cryptoHtml;

    if (wallet.btcInBtcPool > 0 || wallet.dfiInBtcPool > 0 || wallet.btc > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.btc, wallet.btcInBtcPool, wallet.dfiInBtcPool, index, 'BTC', 'DFI');
        index ++;
    }

    if (wallet.ethInEthPool > 0 || wallet.dfiInEthPool > 0 || wallet.eth > 0) {
        cryptoHtmlResult = cryptoHtmlResult +replacePoolItem(contentHtmlCrypto, wallet.eth, wallet.ethInEthPool, wallet.dfiInEthPool, index, 'ETH', 'DFI');
        index ++;
    }

    if (wallet.ltcInLtcPool > 0 || wallet.dfiInLtcPool > 0 || wallet.eth > 0) {
        cryptoHtmlResult = cryptoHtmlResult +replacePoolItem(contentHtmlCrypto, wallet.ltc, wallet.ltcInLtcPool, wallet.dfiInLtcPool, index, 'LTC', 'DFI');
        index ++;
    }

    if (wallet.usdtInUsdtPool > 0 || wallet.dfiInUsdtPool > 0 || wallet.usdt > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.usdt, wallet.usdtInUsdtPool, wallet.dfiInUsdtPool, index, 'USDT', 'DFI');
        index ++;
    }

    if (wallet.usdcInUsdcPool > 0 || wallet.dfiInUsdcPool > 0 || wallet.usdc > 0) {
        cryptoHtmlResult = cryptoHtmlResult +replacePoolItem(contentHtmlCrypto, wallet.usdc, wallet.usdcInUsdcPool, wallet.dfiInUsdcPool, index, 'USDC', 'DFI');
        index ++;
    }

    if (wallet.bchInBchPool > 0 || wallet.dfiInBchPool > 0 || wallet.bch > 0) {
        cryptoHtmlResult = cryptoHtmlResult +replacePoolItem(contentHtmlCrypto, wallet.bch, wallet.bchInBchPool, wallet.dfiInBchPool, index, 'BCH', 'DFI');
        index ++;
    }

    if (wallet.dogeInDogePool > 0 || wallet.dfiInDogePool > 0 || wallet.doge > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.doge, wallet.dogeInDogePool, wallet.dfiInDogePool, index, 'DOGE', 'DFI');
        index ++;
    }

    contentHtml = contentHtml.replace("{{cryptos}}", cryptoHtmlResult);

    return contentHtml;

}

function stocks(contentHtml, wallet) {

    const template = fs.readFileSync(__dirname + '/templates/wallet.mjml', {encoding: 'utf8', flag: 'r'});
    let cryptoHtml = template;
    let cryptoHtmlResult = "";
    let index = 1;

    let contentHtmlCrypto = cryptoHtml;

    if (wallet.usdInUsdPool > 0 || wallet.dfiInUsdPool > 0 || wallet.usd > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.usd, wallet.usdInUsdPool, wallet.dfiInUsdPool, index, 'DUSD', 'DFI');
        index ++;
    }

    if (wallet.spyInSpyPool > 0 || wallet.usdInSpyPool > 0 || wallet.spy > 0) {
        cryptoHtmlResult = cryptoHtmlResult +replacePoolItem(contentHtmlCrypto, wallet.spy, wallet.spyInSpyPool, wallet.usdInSpyPool, index, 'SPY', 'DUSD');
        index ++;
    }

    if (wallet.qqqInQqqPool > 0 || wallet.usdInQqqPool > 0 || wallet.qqq > 0) {
        cryptoHtmlResult = cryptoHtmlResult +replacePoolItem(contentHtmlCrypto, wallet.qqq, wallet.qqqInQqqPool, wallet.usdInQqqPool, index, 'QQQ', 'DUSD');
        index ++;
    }

    if (wallet.tslaInTslaPool > 0 || wallet.usdInTslaPool > 0 || wallet.tsla > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.tsla, wallet.tslaInTslaPool, wallet.usdInTslaPool, index, 'TSLA', 'DUSD');
        index ++;
    }

    if (wallet.aaplInAaplPool > 0 || wallet.usdInAaplPool > 0 || wallet.aapl > 0) {
        cryptoHtmlResult = cryptoHtmlResult +replacePoolItem(contentHtmlCrypto, wallet.aapl, wallet.aaplInAaplPool, wallet.usdInAaplPool, index, 'AAPL', 'DUSD');
        index ++;
    }

    if (wallet.nvdaInNvdaPool > 0 || wallet.usdInNvdaPool > 0 || wallet.nvda > 0) {
        cryptoHtmlResult = cryptoHtmlResult +replacePoolItem(contentHtmlCrypto, wallet.nvda, wallet.nvdaInNvdaPool, wallet.usdInNvdaPool, index, 'NVDA', 'DUSD');
        index ++;
    }

    if (wallet.gmeInGmePool > 0 || wallet.usdInGmePool > 0 || wallet.gme > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.gme, wallet.gmeInGmePool, wallet.usdInGmePool, index, 'GME', 'DUSD');
        index ++;
    }

    if (wallet.coinInCoinPool > 0 || wallet.usdInCoinPool > 0 || wallet.coin > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.coin, wallet.coinInCoinPool, wallet.usdInCoinPool, index, 'COIN', 'DUSD');
        index ++;
    }

    if (wallet.amznInAmznPool > 0 || wallet.usdInAmznPool > 0 || wallet.amzn > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.amzn, wallet.amznInAmznPool, wallet.usdInAmznPool, index, 'AMZN', 'DUSD');
        index ++;
    }

    if (wallet.babaInBabaPool > 0 || wallet.usdInBabaPool > 0 || wallet.baba > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.baba, wallet.babaInBabaPool, wallet.usdInBabaPool, index, 'BABA', 'DUSD');
        index ++;
    }

    if (wallet.arkkInArkkPool > 0 || wallet.usdInArkkPool > 0 || wallet.arkk > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.arkk, wallet.arkkInArkkPool, wallet.usdInArkkPool, index, 'ARKK', 'DUSD');
        index ++;
    }

    if (wallet.pltrInPltrPool > 0 || wallet.usdInPltrPool > 0 || wallet.pltr > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.pltr, wallet.pltrInPltrPool, wallet.usdInPltrPool, index, 'PLTR', 'DUSD');
        index ++;
    }

    if (wallet.googlInGooglPool > 0 || wallet.usdInGooglPool > 0 || wallet.googl > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.googl, wallet.googlInGooglPool, wallet.usdInGooglPool, index, 'GOOGL', 'DUSD');
        index ++;
    }

    if (wallet.eemInEemPool > 0 || wallet.usdInEemPool > 0 || wallet.eem > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.eem, wallet.eemInEemPool, wallet.usdInEemPool, index, 'EEM', 'DUSD');
        index ++;
    }

    if (wallet.tltInTltPool > 0 || wallet.usdInTltPool > 0 || wallet.tlt > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.tlt, wallet.tltInTltPool, wallet.usdInTltPool, index, 'TLT', 'DUSD');
        index ++;
    }

    if (wallet.slvInSlvPool > 0 || wallet.usdInSlvPool > 0 || wallet.slv > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.slv, wallet.slvInSlvPool, wallet.usdInSlvPool, index, 'SLV', 'DUSD');
        index ++;
    }

    if (wallet.gldInGldPool > 0 || wallet.usdInGldPool > 0 || wallet.gld > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.gld, wallet.gldInGldPool, wallet.usdInGldPool, index, 'GLD', 'DUSD');
        index ++;
    }

    if (wallet.vnqInVnqPool > 0 || wallet.usdInVnqPool > 0 || wallet.vnq > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.vnq, wallet.vnqInVnqPool, wallet.usdInVnqPool, index, 'VNQ', 'DUSD');
        index ++;
    }

    if (wallet.urthInUrthPool > 0 || wallet.usdInUrthPool > 0 || wallet.urth > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.urth, wallet.urthInUrthPool, wallet.usdInUrthPool, index, 'URTH', 'DUSD');
        index ++;
    }

    if (wallet.pdbcInPdbcPool > 0 || wallet.usdInPdbcPool > 0 || wallet.pdbc > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.pdbc, wallet.pdbcInPdbcPool, wallet.usdInPdbcPool, index, 'PDBC', 'DUSD');
        index ++;
    }

    if (wallet.msftInMsftPool > 0 || wallet.usdInMsftPool > 0 || wallet.msft > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.msft, wallet.msftInMsftPool, wallet.usdInMsftPool, index, 'MSFT', 'DUSD');
        index ++;
    }

    if (wallet.vooInVooPool > 0 || wallet.usdInVooPool > 0 || wallet.voo > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.voo, wallet.vooInVooPool, wallet.usdInVooPool, index, 'VOO', 'DUSD');
        index ++;
    }

    if (wallet.fbInFbPool > 0 || wallet.usdInFbPool > 0 || wallet.fb > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.fb, wallet.fbInFbPool, wallet.usdInFbPool, index, 'FB', 'DUSD');
        index ++;
    }

    if (wallet.nflxInNflxPool > 0 || wallet.usdInNflxPool > 0 || wallet.nflx > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.nflx, wallet.nflxInNflxPool, wallet.usdInNflxPool, index, 'NFLX', 'DUSD');
        index ++;
    }

    contentHtml = contentHtml.replace("{{stocks}}", cryptoHtmlResult);

    return contentHtml;

}

function accountInfoForNewsletter(contentHtml, user, price, pools) {
    contentHtml = contentHtml.replace("{{account}}", user.key);
    contentHtml = contentHtml.replace("{{value}}", round2(user.totalValue));
    contentHtml = contentHtml.replace("{{incomeDfi}}", round2(user.totalValueIncomeDfi));
    contentHtml = contentHtml.replace("{{incomeUsd}}", round2(user.totalValueIncomeUsd));
    const cexDfiPrice = price?.price.aggregated.amount;
    const btcPool = pools.find(p => p.id === "5");
    const dexDfiPrice = btcPool.totalLiquidity.usd / btcPool.tokenB.reserve / 2;
    const premium = (dexDfiPrice / cexDfiPrice * 100) - 100;
    contentHtml = contentHtml.replace("{{dfiPrice}}", "" + round2(cexDfiPrice));
    contentHtml = contentHtml.replace("{{dfiPriceDex}}", "" + round2(dexDfiPrice));
    contentHtml = contentHtml.replace("{{dfiPremium}}", "" + round2(premium));
    return contentHtml;
}

async function vaults(user, contentHtml) {
    const vaults = []
    for (let i = 0; i < user.addresses.length; i++) {

        if (user.addresses[i].length === 0) {
            continue;
        }

        const vaultsAddress = await client.address.listVault(user.addresses[i]).catch( reason =>{
                logger.error("Error during vault request for address " + user.addresses[i] + " reason " + reason);
        });
        if (vaultsAddress && vaultsAddress.length > 0) {
            for (let j = 0; j < vaultsAddress.length; j++) {
                vaults.push(vaultsAddress[j]);
            }
        }


    }

    if (vaults && vaults.length > 0) {
        const templateVault = fs.readFileSync(__dirname + '/templates/vault.mjml', {encoding: 'utf8', flag: 'r'});
        let vaultsHtml = templateVault;
        let vaultsHtmlResult = "";

        for (let i = 0; i < vaults.length; i++) {
            let contentHtmlVault = vaultsHtml;
            contentHtmlVault = contentHtmlVault.replace("{{vaultId}}", vaults[i].vaultId);
            const nameVault = "..." + vaults[i].vaultId.slice(vaults[i].vaultId.length - 5, vaults[i].vaultId.length)
                + " - " + vaults[i].state + " - " + vaults[i].collateralRatio + " %/" + vaults[i].loanScheme.minColRatio + "%";
            contentHtmlVault = contentHtmlVault.replace("{{vault}}", nameVault);
            vaultsHtmlResult = vaultsHtmlResult + contentHtmlVault;
        }

        return contentHtml.replace("{{vaults}}", vaultsHtmlResult);
    } else {
        return contentHtml.replace("{{vaults}}", "");
    }
}

async function sendNewsletterMail(user, stats, price, pools, prices, cake, exchanges) {

    try {

        fs.readFile(__dirname + '/templates/newsletter.mjml', 'utf8', async function read(err, data) {

            if (err) {
                logger.error("Read template newsletter error");
                logger.error(err.message);
                return;
            }

            let balanceMasternodeUtxo = 0;
            let balanceMasternodeToken = 0;
            for (let i = 0; i < user.addressesMasternodes.length; i++) {
                balanceMasternodeUtxo += +await client.address.getBalance(user.addressesMasternodes[i]);
                balanceMasternodeToken += +(await client.address.listToken(user.addressesMasternodes[i])).filter(t => t.id === "0");
            }
            const wallet = user.wallet;
            const dfiInLm = wallet.dfiInBtcPool + wallet.dfiInEthPool + wallet.dfiInUsdtPool + wallet.dfiInUsdcPool
                + wallet.dfiInLtcPool + wallet.dfiInDogePool + wallet.dfiInBchPool + wallet.dfiInUsdPool;

            let contentHtml = data.toString();

            // basic infos of account
            contentHtml = accountInfoForNewsletter(contentHtml, user, price, pools);

            // crypto tokens
            contentHtml = crypto(contentHtml, wallet);

            // stocks tokens
            contentHtml = stocks(contentHtml, wallet);

            // dfi
            contentHtml = dfiForNewsletter(contentHtml, wallet, dfiInLm, balanceMasternodeToken, balanceMasternodeUtxo);

            // Vaults
            contentHtml = await vaults(user, contentHtml);

            // Statistics
            contentHtml = statisticsForNewsletter(contentHtml, stats, pools, prices);

            // staking
            contentHtml = statisticsForStaking(contentHtml, stats, cake);

            // exchanges
            contentHtml = statisticsForExchnges(contentHtml, exchanges);

            const result = await sendMail(user.newsletter.email, "Newsletter - Daily Defichain Statistics", mjml2html(contentHtml).html);

        });

    } catch (err) {
        logger.error("sendNewsletterMail error for user: " + user.key, err)
    }

}

function round(number) {
    const nf = Intl.NumberFormat();
    return number > 0 ? nf.format(Math.round(number * 1000) / 1000) : 0;
}

function round2(number) {
    const nf = Intl.NumberFormat();
    return number > 0 ? nf.format(Math.round(number * 100) / 100) : 0;
}

function roundNeg(number) {
    const nf = Intl.NumberFormat();
    return nf.format(Math.round(number * 100) / 100);
}


async function sendMail(receiver, subject, content) {
    try {

        logger.info("Mail sending start  to " + receiver);

        const mail = {
            from: "DeFiChain-Income.com <defichain-income@topiet.de>",
            to: receiver,
            subject: subject,
            text: content,
            html: content,
        };

        const result = await mailer.sendMail(mail, (error, info) => {
            if (error) {
                return logger.error("Mail sending FAILED to " + receiver + " reason: " + error);
            }
            logger.info("Mail sending SUCCESS to " + receiver + " ID " + info.messageId);
        });

    } catch (err) {
        logger.error("Send Mail error for receiver: " + receiver, err)
    }
}

async function checkNewsletterPayed(address) {
    const dateNow = new Date();
    let transactions = [];

    let response = await client.address.listTransaction(payingAddress, 200)

    for (const item of response) {
        transactions.push(item);
    }

    while (response.hasNext) {
        response = await client.paginate(response)
        for (const item of response) {
            transactions.push(item);
        }
    }

    const network = MainNet.name
    let foundSource = false;
    let foundTarget = false;
    let foundTargetPayed = false;
    for (const t of transactions) {
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

async function saveFarmingPool(dataPairs, oceanPairs) {
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
        pool.volume24h = oceanPairs.data.find(x => x.id === pool.poolId).volume.h24;
        pool.volume30d = oceanPairs.data.find(x => x.id === pool.poolId).volume.d30;

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

        Promise.all([getPool("5"), getPool("4"), getPool("6"), getPool("10"), getPool("8"), getPool("12"), getPool("14"), getPool("17"), getPool("18"), getPoolPairs(), getOceanPoolPairs()])
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
                const oceanPairs = results[10];
                saveFarmingPool(pairs.data, oceanPairs.data).catch(function (error) {
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

                if (u.totalValue > 0 && u.totalValueIncomeDfi > 0 && u.totalValueIncomeUsd > 0) {

                    // Load User and History
                    const userHistoryLoaded = await findHistoryByKey(u.key);

                    // Update History
                    if (userHistoryLoaded && u) {
                        const item = {
                            date: new Date(),
                            totalValue: u.totalValue,
                            totalValueIncomeDfi: u.totalValueIncomeDfi,
                            totalValueIncomeUsd: u.totalValueIncomeUsd
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
                            totalValue: u.totalValue,
                            totalValueIncomeDfi: u.totalValueIncomeDfi,
                            totalValueIncomeUsd: u.totalValueIncomeUsd
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

async function executeNewsletter() {
    const millisecondsBefore = new Date().getTime();
    logger.info("===========Newsletter Job: started: " + new Date() + " ================");

    const users = await User.find({newsletter: {$ne: null}});

    logger.info("===========Newsletter Job: Start Sending Newsletter for subscribers: " + users.length + " ================");

    // get stats of blockchain
    const stats = await client.stats.get();
    const price = await client.prices.get("DFI", "USD");
    const pools = await client.poolpairs.list(100);
    const prices = await client.prices.list(1000);
    const cake = await getCakeApy();
    const exchanges = await loadExchangeInfos();

    logger.info("===========Newsletter all infos loaded ✅ ================");

    let mail = 0;
    let address = 0;
    let payed = 0;

    const date = new Date();

    for (let i = 0; i < users.length; i++) {

        const u = users[i];

        try {

            logger.info("===========Newsletter start for user:  " + u.key + " ✅ ================");

            // kalkuliere Status
            let status = "";

            // 1. Subscribed
            const emailSet = u.newsletter.email && u.newsletter.email.length > 0;
            const addressSet = u.newsletter.payingAddress && u.newsletter.payingAddress.length > 0;
            if (emailSet) {
                status = "subscribed";
                mail++;
            }

            // 2. Subscribed & Address
            if (emailSet && addressSet) {
                status = "subscribedWithAddress";
                address++;
            }

            // 2.1
            if (emailSet && !addressSet) {
                status = "subscribedWithoutAddress";
                logger.info("===========Newsletter user not payed:  " + u.key + " ⚠️ ================");
                await sendNewsletterReminderMail(u.newsletter.email);
                logger.info("===========Newsletter user not payed Reminder sent:  " + u.key + " ⚠️ ================");
            }

            // 3. Payed
            if (emailSet && addressSet) {
                const payedOk = await checkNewsletterPayed(u.newsletter.payingAddress);
                if (payedOk) {
                    status = "payed";
                    payed++;
                    logger.info("===========Newsletter start for payed user:  " + u.key + " ✅ ================");
                    await sendNewsletterMail(u, stats, price, pools, prices, cake, exchanges);
                    logger.info("============ Newsletter finished for payed user: " + u.key + " ✅ ================");
                } else {
                    logger.info("===========Newsletter user not payed:  " + u.key + " ⚠️ ================");

                    if (date.getDay() === 2 || date.getDay() === 5) {
                        logger.info("===========Newsletter user not payed Reminder sent:  " + u.key + " ⚠️ ================");
                        await sendNewsletterReminderMail(u.newsletter.email);
                    }
                }
            }
            u.newsletter.status = status;

            await u.save();

            logger.info("===========Newsletter ready for user:  " + u.key + " ✅ ================");

        } catch (e) {
            logger.error("============ Newsletter Job error for user with key " + u.key + e.message);
        }
    }

    logger.info("===========Newsletter Subscriber send report Mail  ================");
    await sendReportNewsletterMail("crypto@shelkovenkov.de", mail, address, payed)

    logger.info("===========Newsletter Subscriber " + users.length + " with Mails: " + mail + " ================");
    logger.info("===========Newsletter Subscriber with Addresses " + address + " ================");
    logger.info("===========Newsletter Subscriber payed " + payed + " ================");

    const millisecondsAfter = new Date().getTime();
    const msTime = millisecondsAfter - millisecondsBefore;

    logger.info("============Newsletter Job executed time: " + new Date() + " in " + msTime + " ms.=============");
}

if (process.env.JOB_SCHEDULER_ON_NEWSLETTER === "on") {
    schedule.scheduleJob(process.env.JOB_SCHEDULER_TURNUS_NEWSLETTER, async function () {
        await executeNewsletter();

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


