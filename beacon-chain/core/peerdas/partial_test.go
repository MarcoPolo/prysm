package peerdas

import (
	"bytes"
	"testing"

	fieldparams "github.com/OffchainLabs/prysm/v6/config/fieldparams"
	ethpb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
)

// DataColumnSidecarInvariantChecker implements the InvariantChecker interface
// for testing DataColumnSidecar with the partial message invariants.
type DataColumnSidecarInvariantChecker struct{}

// SplitIntoParts splits a full DataColumnSidecar into individual cell parts.
func (c *DataColumnSidecarInvariantChecker) SplitIntoParts(in *PartialDataColumnSidecar) ([]*PartialDataColumnSidecar, error) {
	if len(in.Column) == 0 {
		return nil, nil
	}

	var parts []*PartialDataColumnSidecar
	for i, cell := range in.Column {
		if len(cell) > 0 {
			// Create a partial sidecar with only this cell
			part := &PartialDataColumnSidecar{
				PartialDataColumnSidecar: &ethpb.PartialDataColumnSidecar{
					Index:                        in.Index,
					Column:                       make([][]byte, len(in.Column)),
					KzgCommitments:               in.KzgCommitments,
					KzgProofs:                    in.KzgProofs,
					SignedBlockHeader:            in.SignedBlockHeader,
					KzgCommitmentsInclusionProof: in.KzgCommitmentsInclusionProof,
				},
			}
			// Only set this specific cell
			part.Column[i] = cell
			parts = append(parts, part)
		}
	}

	return parts, nil
}

// FullMessage returns a complete DataColumnSidecar with all cells populated.
func (c *DataColumnSidecarInvariantChecker) FullMessage() (*PartialDataColumnSidecar, error) {
	// Create a mock beacon block header
	header := &ethpb.BeaconBlockHeader{
		Slot:          123,
		ProposerIndex: 456,
		ParentRoot:    make([]byte, fieldparams.RootLength),
		StateRoot:     make([]byte, fieldparams.RootLength),
		BodyRoot:      make([]byte, fieldparams.RootLength),
	}

	signedHeader := &ethpb.SignedBeaconBlockHeader{
		Header:    header,
		Signature: make([]byte, 96), // BLS signature length
	}

	// Create a full data column sidecar with 4 cells
	numCells := 4
	kzgCommitments := make([][]byte, numCells)
	kzgProofs := make([][]byte, numCells)

	for i := 0; i < numCells; i++ {
		kzgCommitments[i] = make([]byte, 48) // KZG commitment length
		kzgProofs[i] = make([]byte, 48)      // KZG proof length
	}

	// Create cells with correct size (2048 bytes each)
	cells := make([][]byte, numCells)
	for i := 0; i < numCells; i++ {
		cells[i] = make([]byte, 2048)
		// Fill with some test data
		copy(cells[i], []byte("cell_"+string(rune('0'+i))+"_data"))
	}

	sidecar := &PartialDataColumnSidecar{
		PartialDataColumnSidecar: &ethpb.PartialDataColumnSidecar{
			Index:             0,
			Column:            cells,
			KzgCommitments:    kzgCommitments,
			KzgProofs:         kzgProofs,
			SignedBlockHeader: signedHeader,
			// KzgCommitmentsInclusionProof depth is 4 (as defined in SSZ spec)
			KzgCommitmentsInclusionProof: [][]byte{
				make([]byte, 32),
				make([]byte, 32),
				make([]byte, 32),
				make([]byte, 32),
			},
		},
	}

	return sidecar, nil
}

// EmptyMessage returns an empty DataColumnSidecar with no cells.
func (c *DataColumnSidecarInvariantChecker) EmptyMessage() *PartialDataColumnSidecar {
	// Create a mock beacon block header
	header := &ethpb.BeaconBlockHeader{
		Slot:          123,
		ProposerIndex: 456,
		ParentRoot:    make([]byte, fieldparams.RootLength),
		StateRoot:     make([]byte, fieldparams.RootLength),
		BodyRoot:      make([]byte, fieldparams.RootLength),
	}

	signedHeader := &ethpb.SignedBeaconBlockHeader{
		Header:    header,
		Signature: make([]byte, 96), // BLS signature length
	}

	// Create an empty data column sidecar with 4 nil cells
	numCells := 4
	kzgCommitments := make([][]byte, numCells)
	kzgProofs := make([][]byte, numCells)

	for i := 0; i < numCells; i++ {
		kzgCommitments[i] = make([]byte, 48) // KZG commitment length
		kzgProofs[i] = make([]byte, 48)      // KZG proof length
	}

	// Create nil cells
	cells := make([][]byte, numCells)
	for i := 0; i < numCells; i++ {
		cells[i] = nil // empty cell
	}

	sidecar := &PartialDataColumnSidecar{
		PartialDataColumnSidecar: &ethpb.PartialDataColumnSidecar{
			Index:             0,
			Column:            cells,
			KzgCommitments:    kzgCommitments,
			KzgProofs:         kzgProofs,
			SignedBlockHeader: signedHeader,
			// KzgCommitmentsInclusionProof depth is 4 (as defined in SSZ spec)
			KzgCommitmentsInclusionProof: [][]byte{
				make([]byte, 32),
				make([]byte, 32),
				make([]byte, 32),
				make([]byte, 32),
			},
		},
	}

	return sidecar
}

// ExtendFromBytes extends a DataColumnSidecar from encoded bytes.
func (c *DataColumnSidecarInvariantChecker) ExtendFromBytes(a *PartialDataColumnSidecar, data []byte) (*PartialDataColumnSidecar, error) {
	if len(data) == 0 {
		return a, nil
	}

	// Create a copy to avoid mutating the original
	result := &PartialDataColumnSidecar{
		PartialDataColumnSidecar: &ethpb.PartialDataColumnSidecar{
			Index:                        a.Index,
			Column:                       make([][]byte, len(a.Column)),
			KzgCommitments:               a.KzgCommitments,
			KzgProofs:                    a.KzgProofs,
			SignedBlockHeader:            a.SignedBlockHeader,
			KzgCommitmentsInclusionProof: a.KzgCommitmentsInclusionProof,
		},
	}

	// Copy existing cells
	for i, cell := range a.Column {
		if len(cell) > 0 {
			result.Column[i] = make([]byte, len(cell))
			copy(result.Column[i], cell)
		}
	}

	// Extend with new data
	result.ExtendFromEncodedPartialMessage("test", data)

	return result, nil
}

// Equal compares two DataColumnSidecar instances for equality.
func (c *DataColumnSidecarInvariantChecker) Equal(a, b *PartialDataColumnSidecar) bool {
	if a.Index != b.Index {
		return false
	}

	if len(a.Column) != len(b.Column) {
		return false
	}

	// Compare each cell
	for i := range a.Column {
		if (len(a.Column[i]) == 0) != (len(b.Column[i]) == 0) {
			return false
		}
		if len(a.Column[i]) > 0 && !bytes.Equal(a.Column[i], b.Column[i]) {
			return false
		}
	}

	// Compare KZG commitments and proofs lengths
	if len(a.KzgCommitments) != len(b.KzgCommitments) {
		return false
	}
	if len(a.KzgProofs) != len(b.KzgProofs) {
		return false
	}

	// Compare KZG commitments and proofs
	for i := range a.KzgCommitments {
		if !bytes.Equal(a.KzgCommitments[i], b.KzgCommitments[i]) {
			return false
		}
	}
	for i := range a.KzgProofs {
		if !bytes.Equal(a.KzgProofs[i], b.KzgProofs[i]) {
			return false
		}
	}

	if a.SignedBlockHeader.Header.Slot != b.SignedBlockHeader.Header.Slot {
		return false
	}

	return true
}

// TestDataColumnSidecarInvariants tests the DataColumnSidecar implementation
// against the partial message invariants.
func TestDataColumnSidecarInvariants(t *testing.T) {
	checker := &DataColumnSidecarInvariantChecker{}
	partialmessages.TestPartialMessageInvariants(t, checker)
}

// TestBitmapOperations tests the bitmap helper functions.
func TestBitmapOperations(t *testing.T) {
	t.Run("createBitmap available", func(t *testing.T) {
		column := [][]byte{
			[]byte("cell0"),
			nil,
			[]byte("cell2"),
			nil,
		}

		bitmap := createBitmap(column, true)

		// Should have bits set for indices 0 and 2
		expected := []byte{0b00000101} // bits 0 and 2 set
		if !bytes.Equal(bitmap, expected) {
			t.Errorf("Expected bitmap %08b, got %08b", expected[0], bitmap[0])
		}
	})

	t.Run("createBitmap missing", func(t *testing.T) {
		column := [][]byte{
			[]byte("cell0"),
			nil,
			[]byte("cell2"),
			nil,
		}

		bitmap := createBitmap(column, false)

		// Should have bits set for indices 1 and 3
		expected := []byte{0b00001010} // bits 1 and 3 set
		if !bytes.Equal(bitmap, expected) {
			t.Errorf("Expected bitmap %08b, got %08b", expected[0], bitmap[0])
		}
	})

	t.Run("hasBitsInCommon", func(t *testing.T) {
		bitmap1 := []byte{0b00000101} // bits 0 and 2
		bitmap2 := []byte{0b00001010} // bits 1 and 3
		bitmap3 := []byte{0b00000001} // bit 0

		if hasBitsInCommon(bitmap1, bitmap2) {
			t.Error("bitmap1 and bitmap2 should not have bits in common")
		}

		if !hasBitsInCommon(bitmap1, bitmap3) {
			t.Error("bitmap1 and bitmap3 should have bits in common")
		}
	})

	t.Run("isZeroBitmap", func(t *testing.T) {
		zeroBitmap := []byte{0x00, 0x00}
		nonZeroBitmap := []byte{0x01, 0x00}

		if !isZeroBitmap(zeroBitmap) {
			t.Error("Zero bitmap should be detected as zero")
		}

		if isZeroBitmap(nonZeroBitmap) {
			t.Error("Non-zero bitmap should not be detected as zero")
		}
	})
}

// TestPartialMessageMethods tests individual methods of the PartialMessage interface.
func TestPartialMessageMethods(t *testing.T) {
	checker := &DataColumnSidecarInvariantChecker{}

	t.Run("GroupID", func(t *testing.T) {
		full, err := checker.FullMessage()
		if err != nil {
			t.Fatal(err)
		}

		groupID := full.GroupID()
		if len(groupID) == 0 {
			t.Error("GroupID should not be empty")
		}

		// GroupID should be consistent
		groupID2 := full.GroupID()
		if !bytes.Equal(groupID, groupID2) {
			t.Error("GroupID should be consistent")
		}
	})

	t.Run("AvailableParts and MissingParts", func(t *testing.T) {
		full, err := checker.FullMessage()
		if err != nil {
			t.Fatal(err)
		}

		empty := checker.EmptyMessage()

		// Full message should have available parts
		availableParts, err := full.AvailableParts()
		if err != nil {
			t.Fatal(err)
		}
		if len(availableParts) == 0 {
			t.Error("Full message should have available parts")
		}

		// Full message should have no missing parts
		missingParts, err := full.MissingParts()
		if err != nil {
			t.Fatal(err)
		}
		if len(missingParts) != 0 {
			t.Error("Full message should have no missing parts")
		}

		// Empty message should have no available parts
		emptyAvailable, err := empty.AvailableParts()
		if err != nil {
			t.Fatal(err)
		}
		if len(emptyAvailable) != 0 {
			t.Error("Empty message should have no available parts")
		}

		// Empty message should have missing parts
		emptyMissing, err := empty.MissingParts()
		if err != nil {
			t.Fatal(err)
		}
		if len(emptyMissing) == 0 {
			t.Error("Empty message should have missing parts")
		}
	})
}
