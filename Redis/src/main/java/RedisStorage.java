import org.redisson.Redisson;
import org.redisson.api.RKeys;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.util.Date;

public class RedisStorage {
    private final String KEY = "Users";
    private RKeys rKeys;
    private RScoredSortedSet<String> users;
    private static final int SLEEP = 1;
    private static final int SHOW_TIME = 2;


    private RedisStorage(RedissonClient redisson) {
        users = redisson.getScoredSortedSet(KEY, new StringCodec("UTF-8"));
        rKeys = redisson.getKeys();
        rKeys.delete(KEY);
    }

    public static RedisStorage createConfig(String configAddress) {
        Config config = new Config();
        config.useSingleServer().setAddress(configAddress);
        return (new RedisStorage(Redisson.create(config)));
    }

    public void addNPerson(int n) {
        for (int i = 1; i <= n; i++) {
            users.add(getTs(), String.valueOf(i));
        }
    }

    private double getTs() {
        return new Date().getTime();
    }

    public void writeAllUsers() {
        for (int i = 1; i <= users.size(); i++) {
            System.out.println(i + " - " + users.getScore(String.valueOf(i)));
        }
    }

    public void showUser(String id) throws InterruptedException {
        System.out.println("-- На главной странице показываем пользователя " + id);
        users.add(getTs(), id);
        Thread.sleep(SHOW_TIME);
    }

    public void buyFirstPriorety(String id) throws InterruptedException {
        System.out.println("> Пользователь " + id + " оплатил платную услугу");
        users.add(users.firstScore() - 1, id);
    }

    public int randomInt(int min, int max) {
        return ((int) ((Math.random() * (max - min) + min + 1)));
    }

    public void showUserQueueSim(int oneBuyPrioretyBetween) throws InterruptedException {
        int buyingUser = users.size() / oneBuyPrioretyBetween;
        int maxLimitStart = oneBuyPrioretyBetween;
        int minLimitStart = 0;
        int[] richUsers = new int[buyingUser];
        int[] lastUsers = new int[buyingUser];
        for (int i = 0; i < buyingUser; i++) {
            int richUser = randomInt(minLimitStart,maxLimitStart);
            int lastUser = randomInt(minLimitStart, maxLimitStart);
            while (richUser <= lastUser) {
                richUser = randomInt(minLimitStart, maxLimitStart);
                lastUser = randomInt(minLimitStart, maxLimitStart);
            }
            richUsers[i] = richUser;
            lastUsers[i] = lastUser;
            minLimitStart = maxLimitStart;
            maxLimitStart = maxLimitStart  + maxLimitStart * (i+1);

        }

        System.out.println("\nНовый цикл -> Информация по текущей симуляции\n(оплатил свой приоритет - должен был отобразиться):");
        for (int i = 0; i < richUsers.length; i++) {
            System.out.println(richUsers[i] + " - " + lastUsers[i]);
        }

        for (int j = 1; j <= users.size(); j++) {
            for (int i = 0; i < richUsers.length; i++) {
                if (j == lastUsers[i]) {
                    buyFirstPriorety(String.valueOf(richUsers[i]));
                }
            }
            showUser(users.first());
        }
        for (int i = 0; i < richUsers.length; i++) {
            users.add(users.getScore(String.valueOf(richUsers[i] - 1)) + 1, String.valueOf(richUsers[i]));
        }
        Thread.sleep(SLEEP);
    }
}

