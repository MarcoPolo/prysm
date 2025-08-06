package peerdas

import (
	ethpb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p/core/peer"
)

// PartialDataColumnSidecar represents an initially incomplete
// DataColumnSidecar.
// It may become complete after receiving its missing parts.
type PartialDataColumnSidecar struct {
	*ethpb.PartialDataColumnSidecar

	// PenalizePeer is called when a peer gives us malformed data
	// TODO: use this
	PenalizePeer func(peer.ID)
	// OnNewData is called when we received new data from a peer
	OnNewData func(d *PartialDataColumnSidecar, newData []byte)
	// OnComplete is called when we have the full data column
	OnComplete func(d *PartialDataColumnSidecar)
}

// GroupID implements partialmessages.PartialMessage.
// Returns the block root to group all data column sidecars belonging to the same block.
func (d *PartialDataColumnSidecar) GroupID() []byte {
	if d.SignedBlockHeader == nil || d.SignedBlockHeader.Header == nil {
		return nil
	}

	root, err := d.SignedBlockHeader.Header.HashTreeRoot()
	if err != nil {
		return nil
	}

	return root[:]
}

// AvailableParts implements partialmessages.PartialMessage.
// Returns a bitmap indicating which cells in the column are available.
func (d *PartialDataColumnSidecar) AvailableParts() ([]byte, error) {
	availableBitmap := createBitmap(d.Column, true)
	// If no cells are available, return empty slice
	if isZeroBitmap(availableBitmap) {
		return nil, nil
	}
	return availableBitmap, nil
}

// MissingParts implements partialmessages.PartialMessage.
// Returns a bitmap indicating which cells in the column are missing.
func (d *PartialDataColumnSidecar) MissingParts() ([]byte, error) {
	missingBitmap := createBitmap(d.Column, false)
	// If no cells are missing, return empty slice
	if isZeroBitmap(missingBitmap) {
		return nil, nil
	}
	return missingBitmap, nil
}

// ShouldRequest implements partialmessages.PartialMessage.
// Returns true if the peer has cells that we need.
func (d *PartialDataColumnSidecar) ShouldRequest(from peer.ID, peerHasMetadata []byte) bool {
	if len(peerHasMetadata) == 0 {
		return false
	}

	missingBitmap := createBitmap(d.Column, false)

	// Check if peer has any cells we're missing
	return hasBitsInCommon(peerHasMetadata, missingBitmap)
}

// PartialMessageBytesFromMetadata implements partialmessages.PartialMessage.
// Creates a partial message containing the requested cells.
func (d *PartialDataColumnSidecar) PartialMessageBytesFromMetadata(metadata []byte) ([]byte, []byte, error) {
	// If metadata is empty, treat as request for all parts
	var requestBitmap []byte
	if len(metadata) == 0 {
		if len(d.Column) == 0 {
			return nil, nil, nil
		}
		// Create a bitmap requesting all cells
		bitmapSize := (len(d.Column) + 7) / 8
		requestBitmap = make([]byte, bitmapSize)
		for i := 0; i < len(d.Column); i++ {
			byteIndex := i / 8
			bitIndex := i % 8
			requestBitmap[byteIndex] |= 1 << bitIndex
		}
	} else {
		requestBitmap = metadata
	}

	// Create a partial sidecar with only the requested cells we have
	partialSidecar := &ethpb.PartialDataColumnSidecar{
		Index:  d.Index,
		Column: make([][]byte, len(d.Column)),
		// TODO: only include the commitments/proofs that were requested.
		KzgCommitments:               d.KzgCommitments,
		KzgProofs:                    d.KzgProofs,
		SignedBlockHeader:            d.SignedBlockHeader,
		KzgCommitmentsInclusionProof: d.KzgCommitmentsInclusionProof,
	}

	// Initialize all cells as nil (empty cells should be nil, not empty slices)
	for i := range partialSidecar.Column {
		partialSidecar.Column[i] = nil
	}

	canFulfill := false
	unfulfilled := make([]byte, len(requestBitmap))
	copy(unfulfilled, requestBitmap)

	// Go through each requested cell
	for i, cell := range d.Column {
		if i >= len(requestBitmap)*8 {
			break
		}

		byteIndex := i / 8
		bitIndex := i % 8

		// Check if this cell is requested
		if requestBitmap[byteIndex]&(1<<bitIndex) != 0 {
			// If we have this cell, include it
			if len(cell) > 0 {
				partialSidecar.Column[i] = cell
				canFulfill = true
				// Clear the bit from unfulfilled
				unfulfilled[byteIndex] &= ^(1 << bitIndex)
			}
		}
	}

	// If we can't fulfill any part of the request
	if !canFulfill {
		return nil, requestBitmap, nil
	}

	// Encode the partial sidecar
	encoded, err := partialSidecar.MarshalSSZ()
	if err != nil {
		return nil, requestBitmap, err
	}

	// Return remaining unfulfilled requests
	if isZeroBitmap(unfulfilled) {
		return encoded, nil, nil
	}

	return encoded, unfulfilled, nil
}

// ExtendFromEncodedPartialMessage implements partialmessages.PartialMessage.
// Extends this sidecar with cells from an encoded partial message.
func (d *PartialDataColumnSidecar) ExtendFromEncodedPartialMessage(from peer.ID, data []byte) {
	var extended bool
	// Decode the partial sidecar
	partialSidecar := &ethpb.PartialDataColumnSidecar{}
	if err := partialSidecar.UnmarshalSSZ(data); err != nil {
		// Log error but don't panic - this is called from gossipsub's goroutine
		return
	}

	// Only merge if it's for our column index
	if partialSidecar.Index != d.Index {
		return
	}

	// Initialize our column if it doesn't exist
	if len(d.Column) == 0 {
		d.Column = make([][]byte, len(partialSidecar.Column))
	}

	// Merge cells from the partial sidecar
	var anyMissing bool
	for i, cell := range partialSidecar.Column {
		if i < len(d.Column) && len(cell) > 0 && len(d.Column[i]) == 0 {
			extended = true
			// TODO: verify the cell! (maybe async)
			d.Column[i] = cell
			// TODO: We should get this from the block
			d.KzgCommitments[i] = partialSidecar.KzgCommitments[i]
			d.KzgProofs[i] = partialSidecar.KzgProofs[i]
		}
		if len(d.Column[i]) == 0 {
			anyMissing = true
		}
	}

	if !anyMissing {
		if d.OnComplete != nil {
			d.OnComplete(d)
		}
	} else if extended {
		if d.OnNewData != nil {
			d.OnNewData(d, data)
		}
	}
}

// createBitmap creates a bitmap indicating which cells are available (available=true) or missing (available=false)
func createBitmap(column [][]byte, available bool) []byte {
	if len(column) == 0 {
		return nil
	}

	bitmapSize := (len(column) + 7) / 8
	bitmap := make([]byte, bitmapSize)

	for i, cell := range column {
		byteIndex := i / 8
		bitIndex := i % 8

		cellExists := len(cell) > 0
		if cellExists == available {
			bitmap[byteIndex] |= 1 << bitIndex
		}
	}

	return bitmap
}

// hasBitsInCommon checks if two bitmaps have any common bits set
func hasBitsInCommon(bitmap1, bitmap2 []byte) bool {
	minLen := len(bitmap1)
	if len(bitmap2) < minLen {
		minLen = len(bitmap2)
	}

	for i := 0; i < minLen; i++ {
		if bitmap1[i]&bitmap2[i] != 0 {
			return true
		}
	}

	return false
}

// hasAnyCells checks if the column has any non-empty cells
func hasAnyCells(column [][]byte) bool {
	for _, cell := range column {
		if len(cell) > 0 {
			return true
		}
	}
	return false
}

// isZeroBitmap checks if all bits in the bitmap are zero
func isZeroBitmap(bitmap []byte) bool {
	for _, b := range bitmap {
		if b != 0 {
			return false
		}
	}
	return true
}

var _ partialmessages.PartialMessage = (*PartialDataColumnSidecar)(nil)
