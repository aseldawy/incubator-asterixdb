package edu.uci.ics.asterix.dataflow.data.common;

import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;

// TODO: Should go into hyracks.
public class ListEditDistanceSearchModifier implements IInvertedIndexSearchModifier {

    private int edThresh;

    public ListEditDistanceSearchModifier(int edThresh) {
        this.edThresh = edThresh;
    }

    public int getEdThresh() {
        return edThresh;
    }

    public void setEdThresh(int edThresh) {
        this.edThresh = edThresh;
    }

    @Override
    public int getOccurrenceThreshold(int numQueryTokens) {
        return numQueryTokens - edThresh;
    }

    @Override
    public int getNumPrefixLists(int occurrenceThreshold, int numInvLists) {
        return numInvLists - occurrenceThreshold + 1;
    }

    @Override
    public short getNumTokensLowerBound(short numQueryTokens) {
        return (short) (numQueryTokens - edThresh);
    }

    @Override
    public short getNumTokensUpperBound(short numQueryTokens) {
        return (short) (numQueryTokens + edThresh);
    }
}
