package iisc.dsl.codd.db.oracle;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

public class OracleSystemStatistics implements Serializable {

	/**
	 * Generated serial UID
	 */
	private static final long serialVersionUID = 8066072340398220796L;
	

    protected BigDecimal cpuSpeedNW;
    protected BigDecimal ioSeekTim;
    protected BigDecimal ioTFRSpeed;
    protected BigDecimal cpuSpeed;
    protected BigDecimal sReadTim;
    protected BigDecimal mReadTim;
    protected BigDecimal mbrc;
    protected BigDecimal maxThr;
    protected BigDecimal slaveThr;
    protected BigInteger parallelMaxServers;
    protected boolean skipped;
    
    private static OracleSystemStatistics sysStat = new OracleSystemStatistics();

    /**
     * Constructs a SystemStatistics with the default values.
     */
    private OracleSystemStatistics()
    {
        this.cpuSpeedNW = BigDecimal.ZERO;
        this.ioSeekTim = BigDecimal.ZERO;
        this.ioTFRSpeed = BigDecimal.ZERO;
        this.cpuSpeed = BigDecimal.ZERO;
        this.sReadTim = BigDecimal.ZERO;
        this.mReadTim = BigDecimal.ZERO;
        this.mbrc = BigDecimal.ZERO;
        this.maxThr = BigDecimal.ZERO;
        this.slaveThr = BigDecimal.ZERO;
        this.parallelMaxServers = BigInteger.ZERO;
        this.skipped = true;
    }

    public static OracleSystemStatistics getInstance() {
    	return sysStat;
    }

	public BigDecimal getCpuSpeedNW() {
		return cpuSpeedNW;
	}


	public void setCpuSpeedNW(BigDecimal cpuSpeedNW) {
		this.cpuSpeedNW = cpuSpeedNW;
	}


	public BigDecimal getIoSeekTim() {
		return ioSeekTim;
	}


	public void setIoSeekTim(BigDecimal ioSeekTim) {
		this.ioSeekTim = ioSeekTim;
	}


	public BigDecimal getIoTFRSpeed() {
		return ioTFRSpeed;
	}


	public void setIoTFRSpeed(BigDecimal ioTFRSpeed) {
		this.ioTFRSpeed = ioTFRSpeed;
	}


	public BigDecimal getCpuSpeed() {
		return cpuSpeed;
	}


	public void setCpuSpeed(BigDecimal cpuSpeed) {
		this.cpuSpeed = cpuSpeed;
	}


	public BigDecimal getsReadTim() {
		return sReadTim;
	}


	public void setsReadTim(BigDecimal sReadTim) {
		this.sReadTim = sReadTim;
	}


	public BigDecimal getmReadTim() {
		return mReadTim;
	}


	public void setmReadTim(BigDecimal mReadTim) {
		this.mReadTim = mReadTim;
	}


	public BigDecimal getMbrc() {
		return mbrc;
	}


	public void setMbrc(BigDecimal mbrc) {
		this.mbrc = mbrc;
	}


	public BigDecimal getMaxThr() {
		return maxThr;
	}


	public void setMaxThr(BigDecimal maxThr) {
		this.maxThr = maxThr;
	}


	public BigDecimal getSlaveThr() {
		return slaveThr;
	}


	public void setSlaveThr(BigDecimal slaveThr) {
		this.slaveThr = slaveThr;
	}


	public BigInteger getParallelMaxServers() {
		return parallelMaxServers;
	}


	public void setParallelMaxServers(BigInteger parallelMaxServers) {
		this.parallelMaxServers = parallelMaxServers;
	}

	public boolean isSkipped() {
		return skipped;
	}

	public void setSkipped(boolean skipped) {
		this.skipped = skipped;
	}

}
