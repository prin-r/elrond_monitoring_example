const { Sequelize, Model, DataTypes } = require("sequelize")
const { CronJob } = require("cron")
const axios = require("axios").default
const {
  Address,
  ProxyProvider,
  SmartContract,
  ContractFunction,
  Argument
} = require("@elrondnetwork/erdjs")
const MAX_QUERY_SYMBOLS = 25
const config = require("./config")
const provider = new ProxyProvider(
  config.ELROND_URL,
  config.ELROND_PROXY_PROVIDER_TIMEOUT
)
const { alert, catchIncident } = require("./notification")
const { RelayerBalance, RealWorldPrice, SymbolDetail, ContractPriceDetail, LatestResult } = require("./db")

const sequelize = new Sequelize(config.DATABASE_URL, { logging: false })

const getBalance = async address => {
  try {
    const {
      data: {
        data: {
          account: { balance }
        }
      }
    } = await axios.get(config.ELROND_URL + "/address/" + address)
    return balance
  } catch {
    return 0
  }
}

const updateBalance = async () => {
  let balance = await getBalance(config.RELAYER)
  if (balance < config.BALANCE_THRESHOLD) {
    await alert(
      "Relayer account less that threshold",
      `Balance of ${config.RELAYER} on ${config.TARGET_NETWORK} is ${balance}, please send some tokens before system downed`
    )
  }

  await RelayerBalance.create({
    timestamp: new Date(),
    address: config.RELAYER,
    balance: balance * 1e12
  })
}

const getReferenceDataBulk = async symbols => {
  const sc = new SmartContract({
    address: new Address(config.CONTRACT_ADDRESS)
  })

  let acc = []
  let startIndex = 0
  let retryCount = 0
  while (startIndex < symbols.length) {
    try {
      const queryResponse = await sc.runQuery(provider, {
        func: new ContractFunction("getReferenceDataBulk"),
        args: symbols
          .slice(startIndex, startIndex + MAX_QUERY_SYMBOLS)
          .map(s => [Argument.fromUTF8(s), Argument.fromUTF8("USD")])
          .flat()
      })

      const { returnData } = queryResponse.toJSON()
      acc = [...acc, ...returnData]
      startIndex += MAX_QUERY_SYMBOLS
      retryCount = 0
    } catch (e) {
      alert("Fail to query getReferenceDataBulk", e)
      if (retryCount < config.MAX_RETRY) {
        retryCount++
      } else {
        alert(
          "Reach max retry",
          `MAX_RETRY: ${config.MAX_RETRY}, MAX_QUERY_SYMBOLS: ${MAX_QUERY_SYMBOLS}`
        )
        startIndex += MAX_QUERY_SYMBOLS
        retryCount = 0
      }
    }
    // slepp 3 secs before starting the next
    await new Promise(r => setTimeout(r, 3000))
  }

  let res = []
  for (let i = 0; i < acc.length; i += 3) {
    res = [
      ...res,
      {
        rate: acc[i].asBigInt.toString(),
        last_updated_base: acc[i + 1].asNumber,
        last_updated_quote: acc[i + 2].asNumber
      }
    ]
  }

  return res
}

const updateContractStatus = async () => {
  const details = await SymbolDetail.findAll()
  const symbols = details.map(({ dataValues }) => dataValues.symbol)
  const contractValues = await getReferenceDataBulk(symbols)

  for (const [idx, detail] of details.entries()) {
    const realPrice = await RealWorldPrice.findByPk(detail.symbol)
    const latestResult = await LatestResult.findByPk(detail.symbol)
    const duration =
      (new Date().getTime() -
        new Date(contractValues[idx].last_updated_base * 1000).getTime()) /
      1000
    const deviation = contractValues[idx].rate / realPrice.value
    const timestamp = new Date(contractValues[idx].last_updated_base * 1000)
    const basicDetail = {
      symbol: symbols[idx],
      contract_value: contractValues[idx].rate,
      timestamp: timestamp,
      requestId: latestResult.requestId,
      txHash: latestResult.txHash
    }
    if (duration > 10 * detail.interval) {
      await ContractPriceDetail.upsert(
        Object.assign(basicDetail, { status: "Delay" })
      )
      await alert(
        "The price value from the contract is too old.",
        `Last update of ${detail.symbol} in contract is ${timestamp}`
      )
    } else if (deviation < 0.9 || deviation > 1.1) {
      await ContractPriceDetail.upsert(
        Object.assign(basicDetail, { status: "WrongPrice" })
      )
      await alert(
        "Data derivation from real data source too much",
        `Value of ${detail.symbol} in contract is ${contractValues[idx].rate} but in the real world is ${realPrice.value}`
      )
    } else {
      await ContractPriceDetail.upsert(
        Object.assign(basicDetail, { status: "Ok" })
      )
    }
  }
}

;(async () => {
  await sequelize.sync()
  new CronJob("0 */10 * * * *", catchIncident(updateBalance), null, true)
  new CronJob("0 */10 * * * *", catchIncident(updateContractStatus), null, true)
})()
