import { buildModule } from "@nomicfoundation/hardhat-ignition/modules";

export default buildModule("HashAnchorModule", (m) => {
  const hashAnchor = m.contract("HashAnchor");
  return { hashAnchor };
});
