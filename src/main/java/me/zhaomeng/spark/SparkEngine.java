package me.zhaomeng.spark;

import java.util.Map;

import me.zhaomeng.rmd.simple.SimpleRecommender;

public class SparkEngine {

	private String master = "local[*]";

	private String engineName = "zming-rmd";

	private String dataDir = "data";

	private SimpleRecommender sr = null;

	private Map<String, ? extends Object> topDealers = null;

	public synchronized String adviseEmailByUser(String user, int numEachType) {

			if(sr == null){
				sr = new SimpleRecommender(master, engineName, dataDir);
			}
		
			if(topDealers == null){
				topDealers =  sr.queryAllTypeDealer(numEachType);
			}
			
			topDealers.forEach((k,v) -> {System.out.println(v);});
			
			Map<String, ? extends Object> userInfo = sr.queryMainTypeByUser(user);
			
			userInfo.forEach((k,v) -> {System.out.println(v);});

		return user;
	}

	public String getMaster() {
		return master;
	}

	public void setMaster(String master) {
		this.master = master;
	}

	public String getEngineName() {
		return engineName;
	}

	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	public String getDataDir() {
		return dataDir;
	}

	public void setDataDir(String dataDir) {
		this.dataDir = dataDir;
	}

	public SimpleRecommender getSr() {
		return sr;
	}

	public void setSr(SimpleRecommender sr) {
		this.sr = sr;
	}
}