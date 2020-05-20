public class Main {
    private static final String CONFIG_ADDRESS = "redis://127.0.0.1:6379";
    private static int countOfUsers = 20;
    private static int oneUserBetween = 10;

    public static void main(String[] args) throws InterruptedException {
        RedisStorage redisStorage = RedisStorage.createConfig(CONFIG_ADDRESS);
        redisStorage.addNPerson(countOfUsers);

        for(;;) {
            redisStorage.showUserQueueSim(oneUserBetween);
        }
    }
}
