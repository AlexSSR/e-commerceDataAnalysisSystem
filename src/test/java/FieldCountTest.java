import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * @author : empcl
 * @since : 2018/8/15 18:12
 */
public class FieldCountTest {

    public static void main(String[] args) throws IOException {

        int count = 0;

        // path路径应该是文件夹路径
        String path = args[0];
        File files = new File(path);
        boolean directory = files.isDirectory();
        if (!directory) {
            System.out.println("这个不是文件夹");
            return;
        }
        String[] file = files.list();
        int len = file.length;
        for (int i = 0; i < len; i++) {

            // 存储遍历到的字段
            ArrayList<String> fields = new ArrayList<String>();
            // 用于存储重复的字段
            HashMap<String, Integer> duplFieldMap = new HashMap<String, Integer>();
            // 用于存储字段
            LinkedList fieldsList = getFieldsList();

            String f = file[i];
            FileInputStream fis = new FileInputStream(path + "\\" + f);
            InputStreamReader is = new InputStreamReader(fis);
            BufferedReader br = new BufferedReader(is);
            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.contains("date")) {
                    if (fields.contains("date")) {
                        Integer c = duplFieldMap.get("date");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("date", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("date");
                        fields.add("date");
                        count = count + 1;
                    }
                }
                if (line.contains("0110")) {
                    if (fields.contains("0110")) {
                        Integer c = duplFieldMap.get("0110");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("0110", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("0110");
                        fields.add("0110");
                        count = count + 1;
                    }
                }
                if (line.contains("id_jida")) {
                    if (fields.contains("id_jida")) {
                        Integer c = duplFieldMap.get("id_jida");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("id_jida", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("id_jida");
                        fields.add("id_jida");
                        count = count + 1;
                    }
                }
                if (line.contains("1112")) {
                    if (fields.contains("1112")) {
                        Integer c = duplFieldMap.get("1112");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("1112", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("1112");
                        fields.add("1112");
                        count = count + 1;
                    }
                }
                if (line.contains("nu_jida")) {
                    if (fields.contains("nu_jida")) {
                        Integer c = duplFieldMap.get("nu_jida");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("nu_jida", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("nu_jida");
                        fields.add("nu_jida");
                        count = count + 1;
                    }
                }
                if (line.contains("2108")) {
                    if (fields.contains("2108")) {
                        Integer c = duplFieldMap.get("2108");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("2108", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("2108");
                        fields.add("2108");
                        count = count + 1;
                    }
                }

                if (line.contains("id_machine")) {
                    if (fields.contains("id_machine")) {
                        Integer c = duplFieldMap.get("id_machine");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("id_machine", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("id_machine");
                        fields.add("id_machine");
                        count = count + 1;
                    }
                }
                if (line.contains("3112")) {
                    if (fields.contains("3112")) {
                        Integer c = duplFieldMap.get("3112");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("3112", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("3112");
                        fields.add("3112");
                        count = count + 1;
                    }
                }
                if (line.contains("id_pur")) {
                    if (fields.contains("id_pur")) {
                        Integer c = duplFieldMap.get("id_pur");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("id_pur", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("id_pur");
                        fields.add("id_pur");
                        count = count + 1;
                    }
                }
                if (line.contains("4100")) {
                    if (fields.contains("4100")) {
                        Integer c = duplFieldMap.get("4100");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("4100", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("4100");
                        fields.add("4100");
                        count = count + 1;
                    }
                }
                if (line.contains("4200")) {
                    if (fields.contains("4200")) {
                        Integer c = duplFieldMap.get("4200");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("4200", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("4200");
                        fields.add("4200");
                        count = count + 1;
                    }
                }
                if (line.contains("area_production")) {
                    if (fields.contains("area_production")) {
                        Integer c = duplFieldMap.get("area_production");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("area_production", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("area_production");
                        fields.add("area_production");
                        count = count + 1;
                    }
                }
                if (line.contains("5300")) {
                    if (fields.contains("5300")) {
                        Integer c = duplFieldMap.get("5300");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("5300", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("5300");
                        fields.add("5300");
                        count = count + 1;
                    }
                }
                if (line.contains("id_car")) {
                    if (fields.contains("id_car")) {
                        Integer c = duplFieldMap.get("id_car");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("id_car", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("id_car");
                        fields.add("id_car");
                        count = count + 1;
                    }
                }
                if (line.contains("7200")) {
                    if (fields.contains("7200")) {
                        Integer c = duplFieldMap.get("7200");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("7200", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("7200");
                        fields.add("7200");
                        count = count + 1;
                    }
                }
                if (line.contains("sum_cost")) {
                    if (fields.contains("sum_cost")) {
                        Integer c = duplFieldMap.get("sum_cost");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("sum_cost", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("sum_cost");
                        fields.add("sum_cost");
                        count = count + 1;
                    }
                }
                if (line.contains("8100")) {
                    if (fields.contains("8100")) {
                        Integer c = duplFieldMap.get("8100");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("8100", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("8100");
                        fields.add("8100");
                        count = count + 1;
                    }
                }
                if (line.contains("8200")) {
                    if (fields.contains("8200")) {
                        Integer c = duplFieldMap.get("8200");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("8200", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("8200");
                        fields.add("8200");
                        count = count + 1;
                    }
                }
                if (line.contains("unit_sale")) {
                    if (fields.contains("unit_sale")) {
                        Integer c = duplFieldMap.get("unit_sale");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("unit_sale", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("unit_sale");
                        fields.add("unit_sale");
                        count = count + 1;
                    }
                }
                if (line.contains("10100")) {
                    if (fields.contains("10100")) {
                        Integer c = duplFieldMap.get("10100");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("10100", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("10100");
                        fields.add("10100");
                        count = count + 1;
                    }
                }
                if (line.contains("call")) {
                    if (fields.contains("call")) {
                        Integer c = duplFieldMap.get("call");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("call", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("call");
                        fields.add("call");
                        count = count + 1;
                    }
                }
                if (line.contains("10200")) {
                    if (fields.contains("10200")) {
                        Integer c = duplFieldMap.get("10200");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("10200", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("10200");
                        fields.add("10200");
                        count = count + 1;
                    }
                }
                if (line.contains("address")) {
                    if (fields.contains("address")) {
                        Integer c = duplFieldMap.get("address");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("address", c + 1);
                        continue;
                    } else {
                        fieldsList.remove("address");
                        fields.add("address");
                        count = count + 1;
                    }
                }
                if (line.contains("11100")) {
                    if (fields.contains("11100")) {
                        Integer c = duplFieldMap.get("11100");
                        if (c == null) {
                            c = 0;
                        }
                        duplFieldMap.put("11100", c + 1);
                    } else {
                        fieldsList.remove("11100");
                        fields.add("11100");
                        count = count + 1;
                    }
                }
            }
            if (count != 24) {
                System.out.println(f + "共出现多少处标注信息：" + count);
                System.out.println(f + "重复数据对应：" + duplFieldMap);
                System.out.println(f + "缺少的标记信息：" + fieldsList);
                System.out.println("----------------分隔符------------------");
                System.out.println();
            }
            count = 0;
        }
    }

    private static LinkedList getFieldsList() {

        LinkedList<String> allfields = new LinkedList<String>();
        allfields.add("date");
        allfields.add("0110");
        allfields.add("id_jida");
        allfields.add("1112");
        allfields.add("nu_jida");
        allfields.add("2108");
        allfields.add("id_machine");
        allfields.add("3112");
        allfields.add("id_pur");
        allfields.add("4100");
        allfields.add("4200");
        allfields.add("area_production");
        allfields.add("5300");
        allfields.add("id_car");
        allfields.add("7200");
        allfields.add("sum_cost");
        allfields.add("8100");
        allfields.add("8200");
        allfields.add("unit_sale");
        allfields.add("10100");
        allfields.add("call");
        allfields.add("10200");
        allfields.add("address");
        allfields.add("11100");

        return allfields;
    }
}