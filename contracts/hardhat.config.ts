import hardhatToolboxMochaEthersPlugin from "@nomicfoundation/hardhat-toolbox-mocha-ethers";
import { configVariable, defineConfig } from "hardhat/config";

export default defineConfig({
  plugins: [hardhatToolboxMochaEthersPlugin],
  solidity: {
    profiles: {
      default: {
        version: "0.8.34",
      },
      production: {
        version: "0.8.34",
        settings: {
          optimizer: {
            enabled: true,
            runs: 200,
          },
        },
      },
    },
  },
  networks: {
    hardhatMainnet: {
      type: "edr-simulated",
      chainType: "l1",
    },
    hardhatOp: {
      type: "edr-simulated",
      chainType: "op",
    },
    // `npx hardhat node` starts a JSON-RPC server on this URL, seeded with
    // 20 funded accounts whose private keys it prints to the console (all
    // derived from Hardhat's well-known public test mnemonic — the same
    // keys every Hardhat/Anvil project uses locally, not a secret). Account
    // #0 below is the first of those and becomes the HashAnchor contract's
    // owner when it deploys.
    localhost: {
      type: "http",
      chainType: "l1",
      url: "http://127.0.0.1:8545",
      accounts: ["0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"],
    },
    // Polygon Amoy testnet. Signs with the same HASHANCHOR_PRIVATE_KEY the
    // backend uses, so the deployer becomes the contract's owner and is the
    // only account allowed to call anchor(). configVariable() reads it from
    // the environment only when a task actually connects to this network,
    // and Hardhat never prints it.
    //
    // The RPC defaults to PublicNode's free endpoint; set AMOY_RPC_URL to
    // use another provider. (Polygon's own rpc-amoy.polygon.technology no
    // longer resolves in DNS.) chainId makes Hardhat refuse to run if that
    // URL turns out to be some other chain.
    amoy: {
      type: "http",
      chainType: "l1",
      chainId: 80002,
      url: process.env.AMOY_RPC_URL ?? "https://polygon-amoy-bor-rpc.publicnode.com",
      accounts: [configVariable("HASHANCHOR_PRIVATE_KEY")],
      ignition: {
        // Amoy's eth_maxPriorityFeePerGas suggestion is skewed by a few
        // transactions paying 500 gwei; nearly everything lands at 30 gwei,
        // just above Polygon's 25 gwei minimum tip. Pinning it here keeps a
        // deploy at roughly 0.01 POL instead of 0.17.
        maxPriorityFeePerGas: 30_000_000_000n,
        maxFeePerGasLimit: 200_000_000_000n,
        explorerUrl: "https://amoy.polygonscan.com",
      },
    },
  },
});
