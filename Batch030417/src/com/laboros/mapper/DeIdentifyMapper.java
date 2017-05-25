package com.laboros.mapper;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeIdentifyMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	
	final Integer[] deIdentifyColumns = {2,4,6,7,8,9};
	private byte[] key="abcdefgh12345678".getBytes();
	

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		//key -- 0
		//value --11111,bbb1,12/10/1950,1234567890,bbb1@xxx.com,
		//1111111111,M,Diabetes,78
		
		final String line = value.toString();
		StringBuffer sb =new StringBuffer();
		
		if(StringUtils.isNotEmpty(line))
		{
			
			List<Integer> columnLst = Arrays.asList(deIdentifyColumns);
			
			final String[] columns = StringUtils.splitPreserveAllTokens(line, ",");
			
			
			String content=null;
			for (int i = 0; i < columns.length; i++) 
			{
			boolean isContains = columnLst.contains(i+1);
			
			if(isContains){
				try {
					content = deIdentifyColumn(columns[i]);
				} catch (InvalidKeyException e) {
					e.printStackTrace();
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				} catch (NoSuchPaddingException e) {
					e.printStackTrace();
				} catch (IllegalBlockSizeException e) {
					e.printStackTrace();
				} catch (BadPaddingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}else{
				content=columns[i];
			}
			sb.append(content);
			if(i!=columns.length)
			{
				sb.append(",");
			}
			}
			
		}
		context.write(new Text(sb.toString()), NullWritable.get());
		
	}
	private String deIdentifyColumn(final String column) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException
	{
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
		cipher.init(Cipher.ENCRYPT_MODE, secretKey);
		String encryptedString = Base64.encodeBase64String(cipher.doFinal(column.getBytes()));
		return encryptedString.trim();
	}
}