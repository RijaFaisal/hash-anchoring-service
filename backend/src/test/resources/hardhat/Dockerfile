# A bare Hardhat node for integration tests (built by HardhatContainer).
#
# Only Hardhat itself is installed — not the contracts/ project's full
# toolchain — because the tests just need a JSON-RPC dev chain; they deploy
# HashAnchor themselves from the web3j wrapper's embedded bytecode. Keep
# HARDHAT_VERSION in step with contracts/package.json.
FROM node:22-slim

ARG HARDHAT_VERSION=3.18.0
WORKDIR /hardhat
RUN npm init -y >/dev/null \
    && npm pkg set type=module \
    && npm install --no-audit --no-fund hardhat@${HARDHAT_VERSION} \
    && echo 'export default {};' > hardhat.config.js

EXPOSE 8545
# Bind to all interfaces so the port is reachable from outside the container.
CMD ["npx", "hardhat", "node", "--hostname", "0.0.0.0", "--port", "8545"]
