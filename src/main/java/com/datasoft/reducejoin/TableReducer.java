package com.datasoft.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {




    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> tableBeans = new ArrayList<TableBean>();
        TableBean pd = new TableBean();

        for (TableBean value: values){
            if ("order".equals(value.getFlag())){
                TableBean tableBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tableBean, value);
                    tableBeans.add(tableBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else {
                try {
                    BeanUtils.copyProperties(pd, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean bean: tableBeans){
            bean.setPname(pd.getPname());
            context.write(bean, NullWritable.get());
        }




    }
}
