import java.io.File;
import java.util.UUID;

/**
 * @author : empcl
 * @since : 2018/8/11 21:19
 */
public class BatchRenameTest {

    public static void main(String[] args) {

        String path = "G:\\车辆资料\\";
        File fileDir = new File(path);
        boolean directory = fileDir.isDirectory();
        if (!directory) {
            System.out.println(path + "不是文件夹！");
            return;
        }

        String[] files = fileDir.list();
        File f = null;
        int len = files.length;
        for (int i = 0; i < len; i++) {
            String oldName = files[i];
            int newName = i + 1;
            f = new File(path + oldName);
            f.renameTo(new File(path + newName+".jpg"));
            f = null;
        }
    }

}
