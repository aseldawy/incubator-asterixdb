package org.apache.asterix.dataflow.data.spatial;

import java.io.EOFException;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.spatial.AbstractUniformGridTokenizer.CellIDToken;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;

/**
 * An abstract tokenizer that partitions data according to a uniform grid.
 * Each subclass should deal with a specific input type.
 * 
 * @author Ahmed Eldawy
 */
public abstract class AbstractUniformGridTokenizer implements IBinaryTokenizer {

    protected double gx1;
    protected double gy1;
    protected double gx2;
    protected double gy2;
    protected int rows;
    protected int columns;

    /** Cell ID to be used as a return value */
    protected CellIDToken currentCellID;

    /**
     * A token that stores an integer cell ID
     * 
     * @author Ahmed Eldawy
     */
    public static class CellIDToken implements IToken {
        /** The value of this token */
        private int cellID;

        /** The raw data of the cell ID value */
        private byte[] data = new byte[4];

        public int getCellID() {
            return cellID;
        }

        public void setCellID(int cellID) {
            this.cellID = cellID;
            // This code follows the same format of DataOutputStream#writeInt
            data[0] = (byte) ((cellID >>> 24) & 0xFF);
            data[1] = (byte) ((cellID >>> 16) & 0xFF);
            data[2] = (byte) ((cellID >>> 8) & 0xFF);
            data[3] = (byte) ((cellID >>> 0) & 0xFF);
        }

        @Override
        public byte[] getData() {
            return data;
        }

        @Override
        public int getEndOffset() {
            return data.length;
        }

        @Override
        public int getStartOffset() {
            return 0;
        }

        @Override
        public int getTokenLength() {
            return data.length;
        }

        @Override
        public void reset(byte[] data, int startOffset, int endOffset, int tokenLength, int tokenCount) {
            System.arraycopy(data, startOffset, this.data, 0, this.data.length);
            int ch1 = Byte.toUnsignedInt(data[0]);
            int ch2 = Byte.toUnsignedInt(data[1]);
            int ch3 = Byte.toUnsignedInt(data[2]);
            int ch4 = Byte.toUnsignedInt(data[3]);
            if ((ch1 | ch2 | ch3 | ch4) < 0)
                throw new RuntimeException("An invalid token value for an integer");
            cellID = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));

        }

        @Override
        public void serializeToken(GrowableArray out) throws IOException {
            out.getDataOutput().write(data);
        }

        @Override
        public void serializeTokenCount(GrowableArray out) throws IOException {
            out.getDataOutput().writeInt(1);
        }

    }

    /**
     * Initializes a new tokenizer given the dimensions of the grid.
     * 
     * @param x1
     *            Minimum x value in the input space
     * @param y1
     *            Minimum y value in the input space
     * @param x2
     *            Maximum x value in the input space
     * @param y2
     *            Maximum y value in the input space
     * @param rows
     *            Total number of rows in the gird
     * @param columns
     *            Total number of columns in the grid
     */
    public AbstractUniformGridTokenizer(double x1, double y1, double x2, double y2, int rows, int columns) {
        this.gx1 = x1;
        this.gy1 = y1;
        this.gx2 = x2;
        this.gy2 = y2;
        this.rows = rows;
        this.columns = columns;
        this.currentCellID = new CellIDToken();
    }

    /**
     * Returns the current value of the token.
     */
    @Override
    public IToken getToken() {
        return currentCellID;
    }

}
