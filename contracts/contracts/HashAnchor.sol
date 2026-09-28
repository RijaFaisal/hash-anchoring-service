// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @title HashAnchor
/// @notice Anchors document hashes on-chain so a document's existence and the
/// block it was anchored in can later be verified without trusting an
/// off-chain database.
contract HashAnchor {
  address public immutable owner;

  struct Anchor {
    bool exists;
    uint64 blockNumber;
  }

  mapping(bytes32 => Anchor) private anchors;

  event HashAnchored(bytes32 indexed docHash, uint64 blockNumber);

  constructor() {
    owner = msg.sender;
  }

  /// @notice Anchors `docHash` at the current block. Owner-only; reverts if
  /// this hash was already anchored. The revert reason "Already anchored" is
  /// relied on by the backend to distinguish a race (event redelivered after
  /// the anchor already succeeded) from a real failure.
  function anchor(bytes32 docHash) external {
    require(msg.sender == owner, "Not authorized");
    require(!anchors[docHash].exists, "Already anchored");

    anchors[docHash] = Anchor({exists: true, blockNumber: uint64(block.number)});
    emit HashAnchored(docHash, uint64(block.number));
  }

  /// @notice Returns whether `docHash` has been anchored and, if so, the
  /// block number it was anchored in (0 if not anchored).
  function verify(bytes32 docHash) external view returns (bool, uint64) {
    Anchor memory a = anchors[docHash];
    return (a.exists, a.blockNumber);
  }
}
