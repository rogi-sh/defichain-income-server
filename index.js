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
    url: 'https://ocean.defichain-income.com',
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
const connectWithRetry = function () {
    return mongoose.connect(process.env.DB_CONN, {dbName: "defichain"}).then(
        () => {
            /** ready to use. The `mongoose.connect()` promise resolves to mongoose instance. */
            logger.info("Connected to Database")
        },
        err => {
            /** handle initial connection error */
            logger.error("Error Connection DB retry in 3s ", err)
            setTimeout(connectWithRetry, 3000);
        }
    );
}
connectWithRetry();

const axios = require('axios');

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
    usdtdusd: Number,
    usdcdusd: Number,
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

    pyplusd: Number,
    brkbusd: Number,
    kousd: Number,
    pgusd: Number,

    sapusd: Number,
    urausd: Number,
    csusd: Number,
    gsgusd: Number,

    ppltusd: Number,
    govtusd: Number,
    tanusd: Number,
    xomusd: Number,

    jnjusd: Number,
    addyyusd: Number,
    gsusd: Number,
    daxusd: Number,

    wmtusd: Number,
    ulusd: Number,
    ungusd: Number,
    usousd: Number,

    dfiInStaking: Number,
    dfiInDfxStaking: Number,

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

    // USDT-DUSD Pool
    usdtInUsdtDusdPool: Number,
    usdtDusd: Number,
    dusdInUsdtDusdPool: Number,

    // USDC Pool
    usdcInUsdcPool: Number,
    usdc: Number,
    dfiInUsdcPool: Number,

    // USDC-DUSD Pool
    usdcInUsdcDusdPool: Number,
    usdcDusd: Number,
    dusdInUsdcDusdPool: Number,

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
    usdInIntcPool: Number,

    // PYPL Pool
    pyplInPyplPool: Number,
    pypl: Number,
    usdInPyplPool: Number,

    // BRKB Pool
    brkbInBrkbPool: Number,
    brkb: Number,
    usdInBrkbPool: Number,

    // KO Pool
    koInKoPool: Number,
    ko: Number,
    usdInKoPool: Number,

    // PG Pool
    pgInPgPool: Number,
    pg: Number,
    usdInPgPool: Number,

    // SAP Pool
    sapInSapPool: Number,
    sap: Number,
    usdInSapPool: Number,

    // GSG Pool
    gsgInGsgPool: Number,
    gsg: Number,
    usdInGsgPool: Number,

    // CS Pool
    csInCsPool: Number,
    cs: Number,
    usdInCsPool: Number,

    // URA Pool
    uraInUraPool: Number,
    ura: Number,
    usdInUraPool: Number,

    // PPLT Pool
    ppltInPpltPool: Number,
    pplt: Number,
    usdInPpltPool: Number,

    // GOVT Pool
    govtInGovtPool: Number,
    govt: Number,
    usdInGovtPool: Number,

    // TAN Pool
    tanInTanPool: Number,
    tan: Number,
    usdInTanPool: Number,

    // XOM Pool
    xomInXomPool: Number,
    xom: Number,
    usdInXomPool: Number,

    // JNJ Pool
    jnjInJnjPool: Number,
    jnj: Number,
    usdInJnjPool: Number,

    // ADDYY Pool
    addyyInAddyyPool: Number,
    addyy: Number,
    usdInAddyyPool: Number,

    // GS Pool
    gsInGsPool: Number,
    gs: Number,
    usdInGsPool: Number,

    // DAX Pool
    daxInDaxPool: Number,
    dax: Number,
    usdInDaxPool: Number,

    // WMT Pool
    wmtInWmtPool: Number,
    wmt: Number,
    usdInWmtPool: Number,

    // UL Pool
    ulInUlPool: Number,
    ul: Number,
    usdInUlPool: Number,

    // UNG Pool
    ungInUngPool: Number,
    ung: Number,
    usdInUngPool: Number,

    // USO Pool
    usoInUsoPool: Number,
    uso: Number,
    usdInUsoPool: Number,

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

const apiKeySchema = new mongoose.Schema({
    name: String,
    bearer: String
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


const poolFarmingSchema = new mongoose.Schema(poolFarming);

const User = mongoose.model("User", userSchema);
const UserTransaction = mongoose.model("UserTransaction", userTransactionsSchema);
const UserHistory = mongoose.model("UserHistory", userHistorySchema);
const ApiKey = mongoose.model("ApiKey", apiKeySchema);

const PoolFarming = mongoose.model("PoolFarming", poolFarmingSchema);
const Stats = mongoose.model("Stats", StatsSchema);

// gql`` parses your string into an AST
const typeDefs = gql`
    scalar Date

    type Wallet {
        dfi: Float

        dfiInStaking: Float
        dfiInDfxStaking: Float
       
        btcdfi : Float
        ethdfi: Float
        ltcdfi: Float
        dogedfi: Float
        usdtdfi: Float
        usdcdfi: Float
        usdtdusd: Float
        usdcdusd: Float
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
        
        pyplusd: Float
        brkbusd: Float
        kousd: Float
        pgusd: Float
        
        sapusd: Float
        urausd: Float
        csusd: Float
        gsgusd: Float
        
        ppltusd: Float
        govtusd: Float
        tanusd: Float
        xomusd: Float
        
        jnjusd: Float
        addyyusd: Float
        gsusd: Float
        daxusd: Float

        wmtusd: Float
        ulusd: Float
        ungusd: Float
        usousd: Float

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
        
        # USDT-DUSD Pool
        usdtInUsdtDusdPool: Float
        usdtDusd: Float
        dusdInUsdtDusdPool: Float

        # USDC Pool
        usdcInUsdcPool: Float
        usdc: Float
        dfiInUsdcPool: Float
        
        # USDC-DUSD Pool
        usdcInUsdcDusdPool: Float
        usdcDusd: Float
        dusdInUsdcDusdPool: Float

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
        
        # PYPL Pool
        pyplInPyplPool: Float
        pypl:  Float
        usdInPyplPool: Float

        # BRKB Pool
        brkbInBrkbPool:  Float
        brkb: Float
        usdInBrkbPool: Float
    
        # KO Pool
        koInKoPool:  Float
        ko:  Float
        usdInKoPool:  Float
    
        # PG Pool
        pgInPgPool: Float
        pg:  Float
        usdInPgPool:  Float
        
        # SAP Pool
        sapInSapPool: Float
        sap: Float
        usdInSapPool: Float

        # GSG Pool
        gsgInGsgPool: Float
        gsg: Float
        usdInGsgPool: Float

        # CS Pool
        csInCsPool: Float
        cs: Float
        usdInCsPool: Float

        # URA Pool
        uraInUraPool: Float
        ura: Float
        usdInUraPool: Float
        
         # PPLT Pool
        ppltInPpltPool: Float
        pplt: Float
        usdInPpltPool: Float

        # GOVT Pool
        govtInGovtPool: Float
        govt: Float
        usdInGovtPool: Float

        # TAN Pool
        tanInTanPool: Float
        tan: Float
        usdInTanPool: Float

        # XOM Pool
        xomInXomPool: Float
        xom: Float
        usdInXomPool: Float
        
        # JNJ Pool
        jnjInJnjPool: Float
        jnj: Float
        usdInJnjPool: Float

        # ADDYY Pool
        addyyInAddyyPool: Float
        addyy: Float
        usdInAddyyPool: Float

        # GS Pool
        gsInGsPool: Float
        gs: Float
        usdInGsPool: Float

        # DAX Pool
        daxInDaxPool: Float
        dax: Float
        usdInDaxPool: Float
        
        #  WMT Pool
        wmtInWmtPool: Float
        wmt: Float
        usdInWmtPool: Float

        #  UL Pool
        ulInUlPool: Float
        ul: Float
        usdInUlPool: Float

        #  UNG Pool
        ungInUngPool: Float
        ung: Float
        usdInUngPool: Float

        #  USO Pool
        usoInUsoPool: Float
        uso: Float
        usdInUsoPool: Float
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
        kucoinStatusDepositErc20: Boolean,
        kucoinStatusWithdrawErc20: Boolean,
        dfxBuy: String,
        dfxSell: String,
        dfxStaking: String,
        huobiStatus: String
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
        getFarmingHistory(from: DateInput!, till: DateInput!): [PoolList]
        getStats: [Stats]
        getCorrelation(days: Int): Correlation
        getStatisticsIncome: Statistics
        getExchangeStatus: ExchangeStatus
        getOracleHistory(token: String!, date: Date): [PriceHistory]
        getDfxStakingAmounts(addresses: [String]): Float
    }

    input WalletInput {
        dfi: Float

        btcdfi : Float
        ethdfi: Float
        ltcdfi: Float
        dogedfi: Float
        usdtdfi: Float
        usdcdfi: Float
        usdtdusd: Float
        usdcdusd: Float
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
        
        pyplusd: Float
        brkbusd: Float
        kousd: Float
        pgusd: Float
        
        sapusd: Float
        urausd: Float
        csusd: Float
        gsgusd: Float
        
        ppltusd: Float
        govtusd: Float
        tanusd: Float
        xomusd: Float
        
        jnjusd: Float
        addyyusd: Float
        gsusd: Float
        daxusd: Float

        wmtusd: Float
        ulusd: Float
        ungusd: Float
        usousd: Float
        
        dfiInStaking: Float
        dfiInDfxStaking: Float

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
        
        # USDT-DUSD Pool
        usdtInUsdtDusdPool: Float
        usdtDusd: Float
        dusdInUsdtDusdPool: Float

        # USDC Pool
        usdcInUsdcPool: Float
        usdc: Float
        dfiInUsdcPool: Float
        
        # USDC-DUSD Pool
        usdcInUsdcDusdPool: Float
        usdcDusd: Float
        dusdInUsdcDusdPool: Float

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
        
        # PYPL Pool
        pyplInPyplPool: Float
        pypl:  Float
        usdInPyplPool: Float

        # BRKB Pool
        brkbInBrkbPool:  Float
        brkb: Float
        usdInBrkbPool: Float
    
        # KO Pool
        koInKoPool:  Float
        ko:  Float
        usdInKoPool:  Float
    
        # PG Pool
        pgInPgPool: Float
        pg:  Float
        usdInPgPool:  Float   
        
        # SAP Pool
        sapInSapPool: Float
        sap: Float
        usdInSapPool: Float

        # GSG Pool
        gsgInGsgPool: Float
        gsg: Float
        usdInGsgPool: Float

        # CS Pool
        csInCsPool: Float
        cs: Float
        usdInCsPool: Float

        # URA Pool
        uraInUraPool: Float
        ura: Float
        usdInUraPool: Float
        
         # PPLT Pool
        ppltInPpltPool: Float
        pplt: Float
        usdInPpltPool: Float

        # GOVT Pool
        govtInGovtPool: Float
        govt: Float
        usdInGovtPool: Float

        # TAN Pool
        tanInTanPool: Float
        tan: Float
        usdInTanPool: Float

        # XOM Pool
        xomInXomPool: Float
        xom: Float
        usdInXomPool: Float
        
        # JNJ Pool
        jnjInJnjPool: Float
        jnj: Float
        usdInJnjPool: Float

        # ADDYY Pool
        addyyInAddyyPool: Float
        addyy: Float
        usdInAddyyPool: Float

        # GS Pool
        gsInGsPool: Float
        gs: Float
        usdInGsPool: Float

        # DAX Pool
        daxInDaxPool: Float
        dax: Float
        usdInDaxPool: Float
        
        #  WMT Pool
        wmtInWmtPool: Float
        wmt: Float
        usdInWmtPool: Float

        #  UL Pool
        ulInUlPool: Float
        ul: Float
        usdInUlPool: Float

        #  UNG Pool
        ungInUngPool: Float
        ung: Float
        usdInUngPool: Float

        #  USO Pool
        usoInUsoPool: Float
        uso: Float
        usdInUsoPool: Float
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

async function getApiKeyDfx() {
    return ApiKey.findOne({name: "DFX"});
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
    let kucoinStatusDepositErc20;
    let kucoinStatusWithdrawErc20;
    let dfxBuy;
    let dfxSell;
    let dfxStaking
    let huobiStatus;

    logger.info("loadExchangeStatus: Start load exchange Status");

    await axios.all([
        getStatusBittrex(), getStatusKucoin(), getStatusDfx(), getStatusHuobi()
    ])
        .then(axios.spread((response, response2, response3, response4) => {

            logger.info("loadExchangeStatus: Responses arrived");
            bittrexStatus = response.data.status;
            bittrexNotice = response.data.notice;
            kucoinStatusDeposit = response2.data.data.chains[0].isDepositEnabled;
            kucoinStatusWithdraw = response2.data.data.chains[0].isWithdrawEnabled;
            kucoinStatusDepositErc20 = response2.data.data.chains[1].isDepositEnabled;
            kucoinStatusWithdrawErc20 = response2.data.data.chains[1].isWithdrawEnabled;
            dfxBuy = response3.data.buy;
            dfxSell = response3.data.sell;
            dfxStaking = response3.data.staking;
            huobiStatus = response4.data.status;

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
        kucoinStatusDepositErc20: kucoinStatusDepositErc20,
        kucoinStatusWithdrawErc20: kucoinStatusWithdrawErc20,
        dfxBuy: dfxBuy,
        dfxSell: dfxSell,
        dfxStaking: dfxStaking,
        huobiStatus: huobiStatus
    };
    return exchangeStatus;
}

async function testNewsletter(saved) {
    const stats = await client.stats.get();
    const price = await client.prices.get("DFI", "USD");
    const pools = await client.poolpairs.list(1000);
    const prices = await client.prices.list(1000);
    const dfx = await getDFXApy().catch(reason => logger.error("getDFXApy in newsletter" + reason.code + reason.message));
    const exchanges = await loadExchangeInfos();
    await sendNewsletterMail(saved, stats, price, pools, prices, dfx, exchanges);
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
        getDfxStakingAmounts: async (obj, {addresses}, {auth}) => {
            try {
                const millisecondsBefore = new Date().getTime();

                let result = 0;

                const key = await getApiKeyDfx();

                await axios.all([
                    getDfxStakingAmount(addresses, key.bearer),
                ])
                    .then(axios.spread((response) => {

                        const amount = response.data;
                        result = amount.totalAmount;

                    }))
                    .catch(function (error) {
                        // handle error
                        if (error.response) {
                            // Request made and server responded
                            logger.error("==================== ERROR getDfxStakingAmount in Call to API BEGIN ====================");
                            logger.error("getDfxStakingAmount", error.response.data);
                            logger.error("getDfxStakingAmount", error.response.status);
                            logger.error("getDfxStakingAmount", error.response.statusText);
                            logger.error("==================== ERROR VisitsSummary in Call to API END ====================");
                        } else if (error.request) {
                            // The request was made but no response was received
                            logger.error("getDfxStakingAmount", error.request);
                        } else {
                            // Something happened in setting up the request that triggered an Error
                            logger.error('getDfxStakingAmount', error.message);
                        }
                    });


                const millisecondsAfter = new Date().getTime();
                const msTime = millisecondsAfter - millisecondsBefore;
                logger.info("getDfxStakingAmount " + new Date() + " called took " + msTime + " ms.");

                return result;
            } catch (e) {
                logger.error("getDfxStakingAmount", e);
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
                    || u.wallet.dfiInStaking > 0 || u.wallet.dfiInDfxStaking > 0) ? 1 : 0;
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
                //await testNewsletter(saved);

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

function getDFXApy() {
    return axios.get(process.env.DFX_API_STAKING);
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

function getVisitors() {
    return axios.get(process.env.VISITS_API);
}

function getDfxStakingAmount(addresses, key) {
    const config = {
        headers: { Authorization: "Bearer " + key }
    };
    return axios.get(process.env.DFX_API_STAKING_INCOME + addresses.toString(), config);
}

function authDfx() {
    const bodyParameters = {
        address: process.env.DFX_API_AUTH_USER,
        signature: process.env.DFX_API_AUTH_SIGN

    };
    return axios.post(process.env.DFX_API_AUTH, bodyParameters);
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

function getStatusHuobi() {
    return axios.get(process.env.HUOBI_API);
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
    contentHtml = contentHtml.replace("{{dfiInDfxStaking}}", round(wallet.dfiInDfxStaking ? wallet.dfiInDfxStaking : 0));
    contentHtml = contentHtml.replace("{{dfiLm}}", round(dfiInLm));
    contentHtml = contentHtml.replace("{{dfiMasternodes}}", round(balanceMasternodeToken + balanceMasternodeUtxo));
    return contentHtml.replace("{{dfiTotal}}", round(dfiInLm + wallet.dfi + wallet.dfiInStaking + wallet.dfiInDfxStaking + balanceMasternodeToken + balanceMasternodeUtxo));
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

        if (splittedPools(pool)) {
            continue;
        }

        const price = pool.id === "17" ? 1: prices.find(p => p.id === (pool.tokenA.symbol + "-USD")).price.aggregated.amount;
        cryptoHtmlResult = cryptoHtmlResult + replacePoolStatisticsItem(cryptoHtml, pool, index, price);
        index++;
    }

    contentHtml = contentHtml.replace("{{pools}}", cryptoHtmlResult);


    return contentHtml;
}

function splittedPools(pool) {
    // v1 = splitted pools
    // id 123 is dusd burn pool
    // cs is cakes pool
    return pool.symbol.includes("v1") || pool.id === "123" || pool.symbol.startsWith("cs");
}

function statisticsForStaking(contentHtml, stats, dfx) {

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

    const dfxApy = dfx.data ? +dfx.data.staking.yield.apy * 100 : 0;
    const dfxApr = dfx.data  ? +dfx.data.staking.yield.apr * 100 : 0;

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
    contentHtml = contentHtml.replace("{{apyMNDFX}}", round2(dfxApy));
    contentHtml = contentHtml.replace("{{aprMNDFX}}", round2(dfxApr));

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
    contentHtml = contentHtml.replace("{{kucoinDepositErc20}}", exchanges.kucoinStatusDepositErc20 ? 'ONLINE' : 'OFFLINE');
    contentHtml = contentHtml.replace("{{kucoinWithdrawErc20}}", exchanges.kucoinStatusWithdrawErc20 ? 'ONLINE' : 'OFFLINE');

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

    if (wallet.usdtInUsdtDusdPool > 0 || wallet.dusdInUsdtDusdPool > 0 || wallet.usdtDusd > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.usdtDusd, wallet.usdtInUsdtDusdPool, wallet.dusdInUsdtDusdPool, index, 'USDT', 'DUSD');
        index ++;
    }

    if (wallet.usdcInUsdcPool > 0 || wallet.dfiInUsdcPool > 0 || wallet.usdc > 0) {
        cryptoHtmlResult = cryptoHtmlResult +replacePoolItem(contentHtmlCrypto, wallet.usdc, wallet.usdcInUsdcPool, wallet.dfiInUsdcPool, index, 'USDC', 'DFI');
        index ++;
    }

    if (wallet.usdcInUsdcDusdPool > 0 || wallet.dusdInUsdcDusdPool > 0 || wallet.usdcDusd > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.usdcDusd, wallet.usdcInUsdcDusdPool, wallet.dusdInUsdcDusdPool, index, 'USDC', 'DUSD');
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

    if (wallet.disInDisPool > 0 || wallet.usdInDisPool > 0 || wallet.dis > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.dis, wallet.disInDisPool, wallet.usdInDisPool, index, 'DIS', 'DUSD');
        index ++;
    }

    if (wallet.mchiInMchiPool > 0 || wallet.usdInMchiPool > 0 || wallet.mchi > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.mchi, wallet.mchiInMchiPool, wallet.usdInMchiPool, index, 'MCHI', 'DUSD');
        index ++;
    }

    if (wallet.mstrInMstrPool > 0 || wallet.usdInMstrPool > 0 || wallet.mstr > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.mstr, wallet.mstrInMstrPool, wallet.usdInMstrPool, index, 'MSTR', 'DUSD');
        index ++;
    }

    if (wallet.intcInIntcPool > 0 || wallet.usdInIntcPool > 0 || wallet.intc > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.intc, wallet.intcInIntcPool, wallet.usdInIntcPool, index, 'INTC', 'DUSD');
        index ++;
    }

    if (wallet.pyplInPyplPool > 0 || wallet.usdInPyplPool > 0 || wallet.pypl > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.pypl, wallet.pyplInPyplPool, wallet.usdInPyplPool, index, 'PYPL', 'DUSD');
        index ++;
    }

    if (wallet.brkbInBrkbPool > 0 || wallet.usdInBrkbPool > 0 || wallet.brkb > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.brkb, wallet.brkbInBrkbPool, wallet.usdInBrkbPool, index, 'BRK.B', 'DUSD');
        index ++;
    }

    if (wallet.koInKoPool > 0 || wallet.usdInKoPool > 0 || wallet.ko > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.ko, wallet.koInKoPool, wallet.usdInKoPool, index, 'KO', 'DUSD');
        index ++;
    }

    if (wallet.pgInPgPool > 0 || wallet.usdInPgPool > 0 || wallet.pg > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.pg, wallet.pgInPgPool, wallet.usdInPgPool, index, 'PG', 'DUSD');
        index ++;
    }

    if (wallet.sapInSapPool > 0 || wallet.usdInSapPool > 0 || wallet.sap > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.sap, wallet.sapInSapPool, wallet.usdInSapPool, index, 'SAP', 'DUSD');
        index ++;
    }

    if (wallet.gsgInGsgPool > 0 || wallet.usdInGsgPool > 0 || wallet.gsg > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.gsg, wallet.gsgInGsgPool, wallet.usdInGsgPool, index, 'GSG', 'DUSD');
        index ++;
    }

    if (wallet.csInCsPool > 0 || wallet.usdInCsPool > 0 || wallet.cs > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.cs, wallet.csInCsPool, wallet.usdInCsPool, index, 'CS', 'DUSD');
        index ++;
    }

    if (wallet.uraInUraPool > 0 || wallet.usdInUraPool > 0 || wallet.ura > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.ura, wallet.uraInUraPool, wallet.usdInUraPool, index, 'URA', 'DUSD');
        index ++;
    }

    if (wallet.ppltInPpltPool > 0 || wallet.usdInPpltPool > 0 || wallet.pplt > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.pplt, wallet.ppltInPpltPool, wallet.usdInPpltPool, index, 'PPLT', 'DUSD');
        index ++;
    }

    if (wallet.govtInGovtPool > 0 || wallet.usdInGovtPool > 0 || wallet.govt > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.govt, wallet.govtInGovtPool, wallet.usdInGovtPool, index, 'GOVT', 'DUSD');
        index ++;
    }

    if (wallet.tanInTanPool > 0 || wallet.usdInTanPool > 0 || wallet.tan > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.tan, wallet.tanInTanPool, wallet.usdInTanPool, index, 'TAN', 'DUSD');
        index ++;
    }

    if (wallet.xomInXomPool > 0 || wallet.usdInXomPool > 0 || wallet.xom > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.xom, wallet.xomInXomPool, wallet.usdInXomPool, index, 'XOM', 'DUSD');
        index ++;
    }

    if (wallet.jnjInJnjPool > 0 || wallet.usdInJnjPool > 0 || wallet.jnj > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.jnj, wallet.jnjInJnjPool, wallet.usdInJnjPool, index, 'JNJ', 'DUSD');
        index ++;
    }

    if (wallet.addyyInAddyyPool > 0 || wallet.usdInAddyyPool > 0 || wallet.addyy > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.addyy, wallet.addyyInAddyyPool, wallet.usdInAddyyPool, index, 'ADDYY', 'DUSD');
        index ++;
    }

    if (wallet.gsInGsPool > 0 || wallet.usdInGsPool > 0 || wallet.gs > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.gs, wallet.gsInGsPool, wallet.usdInGsPool, index, 'GS', 'DUSD');
        index ++;
    }

    if (wallet.daxInDaxPool > 0 || wallet.usdInDaxPool > 0 || wallet.dax > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.dax, wallet.daxInDaxPool, wallet.usdInDaxPool, index, 'DAX', 'DUSD');
        index ++;
    }

    if (wallet.wmtInWmtPool > 0 || wallet.usdInWmtPool > 0 || wallet.wmt > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.wmt, wallet.wmtInWmtPool, wallet.usdInWmtPool, index, 'WMT', 'DUSD');
        index ++;
    }

    if (wallet.ulInUlPool > 0 || wallet.usdInUlPool > 0 || wallet.ul > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.ul, wallet.ulInUlPool, wallet.usdInUlPool, index, 'UL', 'DUSD');
        index ++;
    }

    if (wallet.ungInUngPool > 0 || wallet.usdInUngPool > 0 || wallet.ung > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.ung, wallet.ungInUngPool, wallet.usdInUngPool, index, 'UNG', 'DUSD');
        index ++;
    }

    if (wallet.usoInUsoPool > 0 || wallet.usdInUsoPool > 0 || wallet.uso > 0) {
        cryptoHtmlResult = cryptoHtmlResult + replacePoolItem(contentHtmlCrypto, wallet.uso, wallet.usoInUsoPool, wallet.usdInUsoPool, index, 'USO', 'DUSD');
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

async function sendNewsletterMail(user, stats, price, pools, prices, dfx, exchanges) {

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
            contentHtml = statisticsForStaking(contentHtml, stats, dfx);

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

async function saveFarmingPool(oceanPairs) {
    const pools = [];

    const usdPool = oceanPairs.data.find(x => x.id === '17');
    const usdPrice = +usdPool.totalLiquidity.usd / 2 / +usdPool.tokenA.reserve;

    oceanPairs.data.forEach(p => {

        if (splittedPools(p)) {
            return;
        }

        let pool = {};
        pool.date = new Date();
        pool.poolId = p.id;
        pool.poolPairId = p.id
        pool.apr = p.apr ? p.apr.total * 100: 0;
        pool.name = p.symbol;
        pool.pair = p.symbol;
        pool.apy = p.apr ? p.apr.total * 100: 0;
        pool.idTokenA = p.tokenA.id;
        pool.idTokenB = p.tokenB.id;
        pool.totalStaked = +p.totalLiquidity.token;
        pool.reserveA = p.tokenA.reserve;
        pool.reserveB = p.tokenB.reserve;
        pool.tokenASymbol = p.tokenA.symbol;
        pool.tokenBSymbol = p.tokenB.symbol;
        pool.priceA = +p.totalLiquidity.usd / 2 / +p.tokenA.reserve;
        pool.priceB = +p.totalLiquidity.usd / 2 / +p.tokenB.reserve;
        pool.totalLiquidityLpToken = +p.totalLiquidity.token;
        pool.totalLiquidity = +p.totalLiquidity.token;
        pool.rewardPct = p.rewardPct;
        pool.commission = p.commission;
        pool.symbol = p.symbol;
        pool.totalLiquidityUsd = +p.totalLiquidity.usd;

        pool.volume24h = p.volume.h24;
        pool.volume30d = p.volume.d30;

        // Crypto Pool price
        if (+p.id < 17) {
            pool.priceA = +p.totalLiquidity.usd / 2 / +p.tokenA.reserve;
            pool.priceB = +p.totalLiquidity.usd / 2 / +p.tokenB.reserve;
        }

        // USD Pool price
        if (+p.id === 17) {
            pool.priceA = usdPrice;
            pool.priceB = +p.totalLiquidity.usd / 2 / +p.tokenB.reserve;
        }

        // Other Stocks
        if (+p.id > 17) {
            pool.priceA = +p.totalLiquidity.usd / 2 / +p.tokenA.reserve;
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

app.get('/income/:address', async function (req, res) {

    const millisecondsBefore = new Date().getTime();
    logger.info("===============Income address started " + new Date() + " =================");

    const address = req.params.address
    const balance = await client.address.getBalance(address);
    logger.info("Balance" + balance)
    let holdings = [];
    holdings.push({
        "amount": balance,
        "symbolKey": "DFI",
        "symbol": "DFI",
        "id": "0",
        "isLps": false,
        "isDat": true,
        "isLoanToken": false
    })
    const token = await client.address.listToken(address, 1000);
    const price = await client.prices.get("DFI", "USD");
    const pools = await client.poolpairs.list(1000);
    const vaults = await client.address.listVault(address, 1000);
    const stats = await client.stats.get();

    const poolUsd = pools.find(p => p.id === "17");
    const poolBtc = pools.find(p => p.id === "5");
    const dUsd = Math.round(price.price.aggregated.amount / poolUsd?.priceRatio.ab  * 1000) / 1000;

    let incomeYearUsd = 0;
    let incomeYearDfi = 0;

    let aprs = 0;
    let lmPoolsIn = 0;

    let lmUsdValue = 0;
    let collateralUsdValue = 0;
    let loanUsdValue = 0;
    let totalValueWallet = balance * +price.price.aggregated.amount;



    for (const t of token) {
        if (t.isLPS) {
            // get pool
            const  pool = pools.find(p => p.id === t.id);
            // compute share
            const share = +t.amount / pool.totalLiquidity.token;
            // compute usd share
            const usdShare = pool.totalLiquidity.usd * share;
            lmUsdValue += usdShare;
            // compute year usd income
            const usdIncome = usdShare * pool.apr.total;
            // compute year dfi income
            const dfiIncome = usdIncome / price.price.aggregated.amount;
            // add to income
            incomeYearUsd += usdIncome;
            incomeYearDfi += dfiIncome;

            aprs += pool.apr.total;
            lmPoolsIn += 1;
        } else if (t.symbol === "DFI"){
            totalValueWallet += +t.amount * +price.price.aggregated.amount;
        } else if (t.symbol === "DUSD") {
            totalValueWallet += +t.amount * dUsd;
        } else {
            const priceToken = await client.prices.get(t.symbol, "USD");
            if (priceToken) {
                totalValueWallet += +t.amount * +priceToken.price.aggregated.amount;
            }
        }

        if (t.id === "0") {
            const poolDfi = holdings.find(h => h.id === "0")
            poolDfi.amount = +poolDfi.amount + +t.amount + "";
            continue;
        }
        holdings.push({
            "amount": t.amount,
            "symbolKey": t.symbolKey,
            "symbol": t.symbol,
            "id": t.id,
            "isLps": t.isLPS,
            "isDat": t.isDAT,
            "isLoanToken": t.isLoanToken
        })

    }

    const dfiHolding = holdings.find(h => h.symbol === "DFI");



    const rewards = {"year": {"usd": incomeYearUsd, "dfi": incomeYearDfi},
                     "month": {"usd": incomeYearUsd / 12, "dfi": incomeYearDfi / 12},
                     "week": {"usd": incomeYearUsd / 52.1429, "dfi": incomeYearDfi / 52.1429},
                     "day": {"usd": incomeYearUsd / 365, "dfi": incomeYearDfi / 365},
                     "hour": {"usd": incomeYearUsd / 8760, "dfi": incomeYearDfi / 8760},
                     "min": {"usd": incomeYearUsd / 525600, "dfi": incomeYearDfi / 525600}};

    const nextOracleInBlocks = 120 - (stats.count.blocks - 1528800) % 120;
    const nextOracleTime = nextOracleInBlocks * 30 / 60;

    let vaultResult = [];
    const vaultsFiltered = vaults.filter(v => v.collateralValue > 0);
    for (const v of vaultsFiltered) {
       collateralUsdValue += +v.collateralValue;
       loanUsdValue += +v.loanValue;
       vaultResult.push(
           { "id": v.vaultId,
             "state": v.state,
             "collateralValue": +v.collateralValue,
             "loanValue": +v.loanValue,
             "vaultRatio": +v.collateralRatio,
             "nextVaultRation":   Math.round(getNextCollateralFromVaultUsd(v) / getNextLoanFromVaultUsd(v) * 100 * 100) / 100
           }
       )
    }

    const avgApr = Math.round(aprs / lmPoolsIn * 100 * 100) / 100;
    const apyDaily = Math.round((100 * (Math.pow(1 + (avgApr / 100 / 365), 365) - 1)) * 100) / 100;
    const apyWeekly = Math.round((100 * (Math.pow(1 + (avgApr / 100 / 52), 52) - 1)) * 100) / 100;

    let poolsCalculated = 0;
    let aprOfAllPools = 0;
    pools.forEach(p => {
        if (!p.symbol.includes("v1") && !p.symbol.includes("DOGE") && !p.symbol.startsWith("cs")) {
            poolsCalculated += 1;
            aprOfAllPools += p.apr.total;
        }
    })
    const aprAvgOfAllPools = Math.round(aprOfAllPools / poolsCalculated * 100 * 100) / 100;

    const response = {
        "totalValueLM": lmUsdValue,
        "totalValueCollateral": collateralUsdValue,
        "totalValueWallet": totalValueWallet,
        "totalValueLoan": loanUsdValue,
        "totalValue": totalValueWallet + lmUsdValue + collateralUsdValue - loanUsdValue,
        "holdings": holdings,
        "rewards": rewards,
        "avgApr":  avgApr,
        "apyDaily": apyDaily,
        "apyWeekly": apyWeekly,
        "aprAvgOfAllPools":aprAvgOfAllPools,
        "dfiPriceOracle": Math.round(+price.price.aggregated.amount * 1000) / 1000,
        "dfiPriceDUSDPool": Math.round(+poolUsd?.priceRatio.ab * 1000) / 1000,
        "dfiPriceBTCPool": Math.round(+poolBtc?.totalLiquidity.usd / 2 / +poolBtc?.tokenB.reserve * 1000) / 1000,
        "dUSD": dUsd,
        "nextOraclePriceBlocks": nextOracleInBlocks,
        "nextOraclePriceTimeInMin": Math.round(nextOracleTime),
        "vaults": vaultResult}

    const millisecondsAfter = new Date().getTime();
    const msTime = millisecondsAfter - millisecondsBefore;

    logger.info("=============Income address executed time: " + new Date() + " in " + msTime + " ms.============");

    res.json(response);
})

function getNextCollateralFromVaultUsd(vault){

    let dfiInVaults = 0;
    let dfiNextPrice = 0;
    let btcInVaults = 0;
    let btcNextPrice = 0;
    let ethInVaults = 0;
    let ethNextPrice = 0;
    let usdcInVaults = 0;
    let usdcNextPrice = 0;
    let usdtInVaults = 0;
    let usdtNextPrice = 0;
    let dusdInVaults = 0;
    const dusdActualPrice = 0.99;

    if (!vault) {
        return 0;
    }

    vault?.collateralAmounts?.forEach(vaultCollaterral => {
        if ('DFI' === vaultCollaterral.symbolKey) {
            dfiInVaults += +vaultCollaterral.amount;
            dfiNextPrice = +vaultCollaterral.activePrice?.next?.amount;
        } else if ('BTC' === vaultCollaterral.symbolKey) {
            btcInVaults += +vaultCollaterral.amount;
            btcNextPrice = +vaultCollaterral.activePrice?.next?.amount;
        } else if ('ETH' === vaultCollaterral.symbolKey) {
            ethInVaults += +vaultCollaterral.amount;
            ethNextPrice = +vaultCollaterral.activePrice?.next?.amount;
        } else if ('USDC' === vaultCollaterral.symbolKey) {
            usdcInVaults += +vaultCollaterral.amount;
            usdcNextPrice = +vaultCollaterral.activePrice?.next?.amount;
        } else if ('USDT' === vaultCollaterral.symbolKey) {
            usdtInVaults += +vaultCollaterral.amount;
            usdtNextPrice = +vaultCollaterral.activePrice?.next?.amount;
        } else if ('DUSD' === vaultCollaterral.symbolKey) {
            dusdInVaults += +vaultCollaterral.amount;
        }

    });

    return dfiInVaults * dfiNextPrice + btcInVaults * btcNextPrice + ethInVaults * ethNextPrice
        + usdcInVaults * usdcNextPrice + usdtInVaults * usdtNextPrice + dusdInVaults * dusdActualPrice;
}

function getNextLoanFromVaultUsd(vault) {

    let usd = 0; let spy = 0; let tsla = 0; let qqq = 0; let pltr = 0; let slv = 0; let aapl = 0; let gld = 0;
    let gme = 0; let google = 0; let arkk = 0; let baba = 0; let vnq = 0; let urth = 0; let tlt = 0;
    let pdbc = 0; let amzn = 0; let nvda = 0; let coin = 0; let eem = 0;
    let msft = 0; let nflx = 0; let fb = 0; let voo = 0;
    let dis = 0; let mchi = 0; let mstr = 0; let intc = 0;
    let pypl = 0; let brkb = 0; let ko = 0; let pg = 0;
    let gsg = 0; let sap = 0; let cs = 0; let ura = 0;
    let xom = 0; let govt = 0; let tan = 0; let pplt = 0;
    let jnj = 0; let addyy = 0; let gs = 0; let dax = 0;
    let wmt = 0; let ul = 0; let ung = 0; let uso = 0;
    let arkx = 0; let vbk = 0; let xle = 0; let xlre = 0;

    vault?.loanAmounts?.forEach(loan => {
        if ('DUSD' === loan.symbolKey) {
            usd = +loan.amount;
        } else if ('SPY' === loan.symbolKey) {
            spy = +loan.amount * +loan.activePrice.next.amount;
        } else if ('TSLA' === loan.symbolKey) {
            tsla = +loan.amount * +loan.activePrice.next.amount;
        } else if ('QQQ' === loan.symbolKey) {
            qqq = +loan.amount * +loan.activePrice.next.amount;
        } else if ('PLTR' === loan.symbolKey) {
            pltr = +loan.amount * +loan.activePrice.next.amount;
        } else if ('SLV' === loan.symbolKey) {
            slv = +loan.amount * +loan.activePrice.next.amount;
        } else if ('AAPL' === loan.symbolKey) {
            aapl = +loan.amount * +loan.activePrice.next.amount;
        } else if ('GLD' === loan.symbolKey) {
            gld = +loan.amount * +loan.activePrice.next.amount;
        } else if ('GME' === loan.symbolKey) {
            gme = +loan.amount * +loan.activePrice.next.amount;
        } else if ('GOOGL' === loan.symbolKey) {
            google = +loan.amount * +loan.activePrice.next.amount;
        } else if ('ARKK' === loan.symbolKey) {
            arkk = +loan.amount * +loan.activePrice.next.amount;
        } else if ('BABA' === loan.symbolKey) {
            baba = +loan.amount * +loan.activePrice.next.amount;
        } else if ('VNQ' === loan.symbolKey) {
            vnq = +loan.amount * +loan.activePrice.next.amount;
        } else if ('URTH' === loan.symbolKey) {
            urth = +loan.amount * +loan.activePrice.next.amount;
        } else if ('TLT' === loan.symbolKey) {
            tlt = +loan.amount * +loan.activePrice.next.amount;
        } else if ('PDBC' === loan.symbolKey) {
            pdbc = +loan.amount * +loan.activePrice.next.amount;
        } else if ('AMZN' === loan.symbolKey) {
            amzn = +loan.amount * +loan.activePrice.next.amount;
        } else if ('NVDA' === loan.symbolKey) {
            nvda = +loan.amount * +loan.activePrice.next.amount;
        } else if ('COIN' === loan.symbolKey) {
            coin = +loan.amount * +loan.activePrice.next.amount;
        } else if ('EEM' === loan.symbolKey) {
            eem = +loan.amount * +loan.activePrice.next.amount;
        } else if ('MSFT' === loan.symbolKey) {
            msft = +loan.amount * +loan.activePrice.next.amount;
        } else if ('NFLX' === loan.symbolKey) {
            nflx = +loan.amount * +loan.activePrice.next.amount;
        } else if ('FB' === loan.symbolKey) {
            fb = +loan.amount * +loan.activePrice.next.amount;
        } else if ('VOO' === loan.symbolKey) {
            voo = +loan.amount * +loan.activePrice.next.amount;
        } else if ('DIS' === loan.symbolKey) {
            dis = +loan.amount * +loan.activePrice.next.amount;
        } else if ('MCHI' === loan.symbolKey) {
            mchi = +loan.amount * +loan.activePrice.next.amount;
        } else if ('MSTR' === loan.symbolKey) {
            mstr = +loan.amount * +loan.activePrice.next.amount;
        } else if ('INTC' === loan.symbolKey) {
            intc = +loan.amount * +loan.activePrice.next.amount;
        } else if ('PYPL' === loan.symbolKey) {
            pypl = +loan.amount * +loan.activePrice.next.amount;
        } else if ('BRK.B' === loan.symbolKey) {
            brkb = +loan.amount * +loan.activePrice.next.amount;
        } else if ('KO' === loan.symbolKey) {
            ko = +loan.amount * +loan.activePrice.next.amount;
        } else if ('PG' === loan.symbolKey) {
            pg = +loan.amount * +loan.activePrice.next.amount;
        } else if ('SAP' === loan.symbolKey) {
            sap = +loan.amount * +loan.activePrice.next.amount;
        } else if ('GSG' === loan.symbolKey) {
            gsg = +loan.amount * +loan.activePrice.next.amount;
        } else if ('CS' === loan.symbolKey) {
            cs = +loan.amount * +loan.activePrice.next.amount;
        } else if ('URA' === loan.symbolKey) {
            ura = +loan.amount * +loan.activePrice.next.amount;
        } else if ('PPLT' === loan.symbolKey) {
            pplt = +loan.amount * +loan.activePrice.next.amount;
        } else if ('GOVT' === loan.symbolKey) {
            govt = +loan.amount * +loan.activePrice.next.amount;
        } else if ('TAN' === loan.symbolKey) {
            tan = +loan.amount * +loan.activePrice.next.amount;
        } else if ('XOM' === loan.symbolKey) {
            xom = +loan.amount * +loan.activePrice.next.amount;
        } else if ('JNJ' === loan.symbolKey) {
            jnj = +loan.amount * +loan.activePrice.next.amount;
        } else if ('ADDYY' === loan.symbolKey) {
            addyy = +loan.amount * +loan.activePrice.next.amount;
        } else if ('GS' === loan.symbolKey) {
            gs = +loan.amount * +loan.activePrice.next.amount;
        } else if ('DAX' === loan.symbolKey) {
            dax = +loan.amount * +loan.activePrice.next.amount;
        }  else if ('WMT' === loan.symbolKey) {
            wmt = +loan.amount * +loan.activePrice.next.amount;
        } else if ('UL' === loan.symbolKey) {
            ul = +loan.amount * +loan.activePrice.next.amount;
        } else if ('UNG' === loan.symbolKey) {
            ung = +loan.amount * +loan.activePrice.next.amount;
        } else if ('USO' === loan.symbolKey) {
            uso = +loan.amount * +loan.activePrice.next.amount;
        }  else if ('ARKX' === loan.symbolKey) {
            arkx = +loan.amount * +loan.activePrice.next.amount;
        } else if ('VBK' === loan.symbolKey) {
            vbk = +loan.amount * +loan.activePrice.next.amount;
        } else if ('XLE' === loan.symbolKey) {
            xle = +loan.amount * +loan.activePrice.next.amount;
        } else if ('XLRE' === loan.symbolKey) {
            xlre = +loan.amount * +loan.activePrice.next.amount;
        }
    });

    return usd + spy + tsla + qqq + pltr + slv + aapl + gld + gme + google + arkk
        + baba + vnq + urth + tlt + pdbc + amzn + nvda + coin + eem + msft + nflx
        + fb + voo + dis + mchi + mstr + intc + pypl + brkb + ko + pg + sap + ura + gsg + cs
        + pplt + xom + govt + tan + jnj + gs + addyy + dax + wmt + ul + ung + uso
        + arkx + vbk + xle + xlre;
}

if (process.env.JOB_SCHEDULER_ON === "on") {
    schedule.scheduleJob(process.env.JOB_SCHEDULER_TURNUS, function () {
        const millisecondsBefore = new Date().getTime();
        logger.info("===============Pools Job started " + new Date() + " =================");

        Promise.all([getOceanPoolPairs()])
            .then(function (results) {
                const oceanPairs = results[0];
                saveFarmingPool(oceanPairs.data).catch(function (error) {
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
    const pools = await client.poolpairs.list(1000);
    const prices = await client.prices.list(1000);
    const dfx = await getDFXApy().catch(reason => logger.error("getDFXApy in newsletter", reason));
    const exchanges = await loadExchangeInfos();

    logger.info("===========Newsletter Job: Newsletter all infos loaded ✅ ================");

    let mail = 0;
    let address = 0;
    let payed = 0;

    const date = new Date();

    for (let i = 0; i < users.length; i++) {

        const u = users[i];

        try {

            logger.info("===========Newsletter Job: start for user:  " + u.key + " ✅ ================");

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
            if (!emailSet && !addressSet) {
                logger.info("===========Newsletter Job: user without mail and addres:  " + u.key + " ⚠️ ================");
            }


            // 2.1
            if (emailSet && !addressSet) {
                status = "subscribedWithoutAddress";
                logger.info("===========Newsletter Job: user not payed:  " + u.key + " ⚠️ ================");
                await sendNewsletterReminderMail(u.newsletter.email);
                logger.info("===========Newsletter Job:  user not payed Reminder sent:  " + u.key + " ⚠️ ================");
            }

            // 3. Payed
            if (emailSet && addressSet) {
                const payedOk = await checkNewsletterPayed(u.newsletter.payingAddress);
                if (payedOk) {
                    status = "payed";
                    payed++;
                    logger.info("===========Newsletter Job: start for payed user:  " + u.key + " ✅ ================");
                    await sendNewsletterMail(u, stats, price, pools, prices, dfx, exchanges);
                    logger.info("============ Newsletter Job: finished for payed user: " + u.key + " ✅ ================");
                } else {
                    logger.info("===========Newsletter Job: user not payed:  " + u.key + " ⚠️ ================");

                    if (date.getDay() === 2 || date.getDay() === 5) {
                        logger.info("===========Newsletter Job: user not payed Reminder sent:  " + u.key + " ⚠️ ================");
                        await sendNewsletterReminderMail(u.newsletter.email);
                    }
                }
            }
            u.newsletter.status = status;

            await u.save();

            logger.info("===========Newsletter Job: ready for user:  " + u.key + " ✅ ================");

        } catch (e) {
            logger.error("============ Newsletter Job: error for user with key " + u.key + e.message);
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

async function authDfxAndSave() {
    await axios.all([
        authDfx(),
    ])
        .then(axios.spread(async (response) => {

            const object = response.data;
            const key = await getApiKeyDfx();
            key.bearer = object.accessToken;
            const saved = await key.save();

        }))
        .catch(function (error) {
            // handle error
            if (error.response) {
                // Request made and server responded
                logger.error("==================== ERROR authDfx in Call to API BEGIN ====================");
                logger.error("authDfx", error.response.data);
                logger.error("authDfx", error.response.status);
                logger.error("authDfx", error.response.statusText);
                logger.error("==================== ERROR authDfx in Call to API END ====================");
            } else if (error.request) {
                // The request was made but no response was received
                logger.error("authDfx", error.request);
            } else {
                // Something happened in setting up the request that triggered an Error
                logger.error('authDfx', error.message);
            }
        });
}

if (process.env.JOB_SCHEDULER_ON_DFX_AUTH === "on") {
    schedule.scheduleJob(process.env.JOB_SCHEDULER_ON_DFX_AUTH_TURNUS, async function () {
        const millisecondsBefore = new Date().getTime();
        logger.info("===========AuthDfxAndSave Job: started: " + new Date() + " ================");
        await authDfxAndSave();
        const millisecondsAfter = new Date().getTime();
        const msTime = millisecondsAfter - millisecondsBefore;

        logger.info("============AuthDfxAndSave Job executed time: " + new Date() + " in " + msTime + " ms.=============");
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
        logger.info("JOB DFX Auth " + process.env.JOB_SCHEDULER_ON_DFX_AUTH + " Cron " + process.env.JOB_SCHEDULER_ON_DFX_AUTH_TURNUS)
        logger.info("DEBUG " + process.env.DEBUG)

    }
);


