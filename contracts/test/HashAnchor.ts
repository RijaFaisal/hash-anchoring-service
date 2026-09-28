import { expect } from "chai";
import { network } from "hardhat";
import { anyValue } from "@nomicfoundation/hardhat-ethers-chai-matchers/withArgs";

const { ethers, networkHelpers } = await network.create();

async function deployHashAnchor() {
  const [owner, other] = await ethers.getSigners();
  const hashAnchor = await ethers.deployContract("HashAnchor");
  return { hashAnchor, owner, other };
}

function hashOf(text: string) {
  return ethers.keccak256(ethers.toUtf8Bytes(text));
}

describe("HashAnchor", function () {
  describe("anchor()", function () {
    it("lets the owner anchor a new hash and emits HashAnchored", async function () {
      const { hashAnchor } = await networkHelpers.loadFixture(deployHashAnchor);
      const docHash = hashOf("contract A");

      await expect(hashAnchor.anchor(docHash))
        .to.emit(hashAnchor, "HashAnchored")
        .withArgs(docHash, anyValue);
    });

    it("records the anchoring block number, readable via verify()", async function () {
      const { hashAnchor } = await networkHelpers.loadFixture(deployHashAnchor);
      const docHash = hashOf("contract A");

      const tx = await hashAnchor.anchor(docHash);
      const receipt = await tx.wait();

      const [exists, blockNumber] = await hashAnchor.verify(docHash);
      expect(exists).to.equal(true);
      expect(blockNumber).to.equal(BigInt(receipt!.blockNumber));
    });

    it("rejects a duplicate anchor of the same hash", async function () {
      const { hashAnchor } = await networkHelpers.loadFixture(deployHashAnchor);
      const docHash = hashOf("contract A");

      await hashAnchor.anchor(docHash);

      await expect(hashAnchor.anchor(docHash)).to.be.revertedWith(
        "Already anchored",
      );
    });

    it("allows anchoring different hashes independently", async function () {
      const { hashAnchor } = await networkHelpers.loadFixture(deployHashAnchor);
      const hashA = hashOf("contract A");
      const hashB = hashOf("contract B");

      await hashAnchor.anchor(hashA);
      await expect(hashAnchor.anchor(hashB)).to.not.revert(ethers);
    });

    it("rejects anchoring from a non-owner account", async function () {
      const { hashAnchor, other } = await networkHelpers.loadFixture(deployHashAnchor);
      const docHash = hashOf("contract A");

      await expect(
        hashAnchor.connect(other).anchor(docHash),
      ).to.be.revertedWith("Not authorized");
    });
  });

  describe("verify()", function () {
    it("returns (false, 0) for a hash that was never anchored", async function () {
      const { hashAnchor } = await networkHelpers.loadFixture(deployHashAnchor);
      const docHash = hashOf("never anchored");

      const [exists, blockNumber] = await hashAnchor.verify(docHash);
      expect(exists).to.equal(false);
      expect(blockNumber).to.equal(0n);
    });

    it("returns (true, blockNumber) for an anchored hash", async function () {
      const { hashAnchor } = await networkHelpers.loadFixture(deployHashAnchor);
      const docHash = hashOf("contract A");

      const tx = await hashAnchor.anchor(docHash);
      const receipt = await tx.wait();

      const [exists, blockNumber] = await hashAnchor.verify(docHash);
      expect(exists).to.equal(true);
      expect(blockNumber).to.equal(BigInt(receipt!.blockNumber));
    });

    it("is callable by non-owner accounts (read-only)", async function () {
      const { hashAnchor, other } = await networkHelpers.loadFixture(deployHashAnchor);
      const docHash = hashOf("contract A");
      await hashAnchor.anchor(docHash);

      const [exists] = await hashAnchor.connect(other).verify(docHash);
      expect(exists).to.equal(true);
    });
  });
});
