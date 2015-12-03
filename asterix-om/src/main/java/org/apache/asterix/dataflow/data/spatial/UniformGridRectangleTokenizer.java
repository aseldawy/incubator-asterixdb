package org.apache.asterix.dataflow.data.spatial;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;

/**
 * Tokenizes rectangles according to a uniform grid. A rectangle is assigned
 * to all grid cells that it partially or completely overlaps. If a rectangle
 * falls partially outside the predefined input space, it is assigned to the
 * overflow cell with ID -1, in addition to all cells that it overlaps.
 * 
 * @author Ahmed Eldawy
 */
public class UniformGridRectangleTokenizer extends AbstractUniformGridTokenizer {

    /** Set to true to indicate that the rectangle should be assigned to the overflow bucket */
    private boolean overflow;

    /**
     * The range of grid cells that overlap the rectangle. The range is open-ended
     * which means [col1, col2[ and [row, row2[
     */
    private int col1, col2, row1, row2;

    /**
     * The current position (row, column) of the iterator. This iterator points
     * to the value that will be returned when the method {@link #next()} is called
     */
    private int irow, icol;

    /**
     * Constructs a tokenizer given the grid information
     * 
     * @param x1
     * @param y1
     * @param x2
     * @param y2
     * @param rows
     * @param columns
     */
    public UniformGridRectangleTokenizer(double x1, double y1, double x2, double y2, int rows, int columns) {
        super(x1, y1, x2, y2, rows, columns);
        this.row2 = this.col2 = -1; // To indicate that there are no
        this.irow = this.icol = -1;
        this.overflow = false;
    }

    @Override
    public boolean hasNext() {
        return overflow || irow < row2;
    }

    @Override
    public void next() {
        // Invalidate the current value as there is no next value
        if (!hasNext())
            return;
        if (overflow) {
            // When the overflow flag is set, it indicates that we didn't return
            // the -1 overflow bucket indicator yet and we need to return it.
            this.currentCellID.setCellID(-1);
            // Invalidate the overflow flag so that we don't return -1 again
            overflow = false;
        } else {
            this.currentCellID.setCellID(irow * columns + icol);
            // Advance to the next column
            icol++;
            if (icol == col2) {
                // End of column, advance to the next row
                icol = col1;
                irow++;
            }
        }
    }

    @Override
    public void reset(byte[] data, int start, int length) {
        double rx1 = ADoubleSerializerDeserializer.getDouble(data, start);
        double ry1 = ADoubleSerializerDeserializer.getDouble(data, start + 8);
        double rx2 = ADoubleSerializerDeserializer.getDouble(data, start + 16);
        double ry2 = ADoubleSerializerDeserializer.getDouble(data, start + 24);

        // For now, I assume that rx1 <= rx2 and ry1 <= ry2. Need to double check
        // with the rectangle format of AsterixDB to make sure that this code
        // works. Otherwise, we can also sort (rx1, rx2) and (ry1, ry2)
        if (rx1 < gx1) {
            rx1 = gx1;
            overflow = true;
        }
        if (rx2 > gx2) {
            rx2 = gx2;
            overflow = true;
        }
        if (ry1 < gx1) {
            ry1 = gx1;
            overflow = true;
        }
        if (ry2 > gy2) {
            ry2 = gy2;
            overflow = true;
        }
        icol = col1 = (int) Math.floor((rx1 - gx1) * columns / (gx2 - gx1));
        col2 = (int) Math.ceil((rx2 - gx1) * columns / (gx2 - gx1));
        irow = row1 = (int) Math.floor((ry1 - gy1) * rows / (gy2 - gy1));
        row2 = (int) Math.ceil((ry2 - gy1) * rows / (gy2 - gy1));
    }

    @Override
    public short getTokensCount() {
        return (short) ((col2 - col1) * (row2 - row1) + (overflow ? 1 : 0));
    }

}
