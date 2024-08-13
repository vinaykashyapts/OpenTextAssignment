import java.util.*;
import java.util.concurrent.*;

/*

Assumptions:
1. Task Order: The order of task submission should be preserved, meaning tasks are processed in the order they were added to the queue.
2. Task Group Exclusivity: Tasks sharing the same TaskGroup are executed one at a time. The tasks are still executed in the order they were submitted, but only one task from the same group will run at a time.
3. Task Execution: Tasks can be executed concurrently if they belong to different groups.

*/

public class Main {

  public enum TaskType {
    READ,
    WRITE,
  }

  public interface TaskExecutor {
    <T> Future<T> submitTask(Task<T> task);
  }

  public record Task<T>(
    UUID taskUUID,
    TaskGroup taskGroup,
    TaskType taskType,
    Callable<T> taskAction
  ) {
    public Task {
      if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
        throw new IllegalArgumentException("All parameters must not be null");
      }
    }
  }

  public record TaskGroup(
    UUID groupUUID
  ) {
    public TaskGroup {
      if (groupUUID == null) {
        throw new IllegalArgumentException("All parameters must not be null");
      }
    }
  }

  public static class TaskExecutorImpl implements TaskExecutor {
    private final ExecutorService executorService;
    private final Map<UUID, BlockingQueue<Runnable>> taskGroupQueues = new ConcurrentHashMap<>();
    private final Set<UUID> runningTaskGroups = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public TaskExecutorImpl(int maxConcurrency) {
      this.executorService = Executors.newFixedThreadPool(maxConcurrency);
    }

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
      BlockingQueue<Runnable> taskQueue = taskGroupQueues.computeIfAbsent(task.taskGroup().groupUUID(), k -> new LinkedBlockingQueue<>());

      TaskRunner<T> taskRunner = new TaskRunner<>(task, taskQueue);

      taskQueue.offer(taskRunner);

      processTaskQueue(task.taskGroup().groupUUID());

      return taskRunner.getFuture();
    }

    private void processTaskQueue(UUID taskGroupUUID) {
      if (runningTaskGroups.contains(taskGroupUUID)) {
        return;
      }

      synchronized (runningTaskGroups) {
        if (runningTaskGroups.contains(taskGroupUUID)) {
          return;
        }

        runningTaskGroups.add(taskGroupUUID);
      }

      BlockingQueue<Runnable> taskQueue = taskGroupQueues.get(taskGroupUUID);

      executorService.submit(() -> {
        try {
          while (true) {
            Runnable taskRunner = taskQueue.poll();

            if (taskRunner == null) {
              break;
            }

            taskRunner.run();
          }
        } finally {
          synchronized (runningTaskGroups) {
            runningTaskGroups.remove(taskGroupUUID);
          }
        }
      });
    }

    private static class TaskRunner<T> implements Runnable {
      private final Task<T> task;
      private final FutureTask<T> futureTask;

      TaskRunner(Task<T> task, BlockingQueue<Runnable> taskQueue) {
        this.task = task;
        this.futureTask = new FutureTask<>(() -> {
          try {
            return task.taskAction().call();
          } finally {
            taskQueue.offer(this); // Re-process the queue after task completion
          }
        });
      }

      @Override
      public void run() {
        futureTask.run();
      }

      public Future<T> getFuture() {
        return futureTask;
      }
    }
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    TaskExecutor executor = new TaskExecutorImpl(5);

    TaskGroup group1 = new TaskGroup(UUID.randomUUID());
    TaskGroup group2 = new TaskGroup(UUID.randomUUID());

    Task<String> task1 = new Task<>(UUID.randomUUID(), group1, TaskType.READ, () -> {
      Thread.sleep(1000);
      return "Task 1 completed";
    });

    Task<String> task2 = new Task<>(UUID.randomUUID(), group2, TaskType.WRITE, () -> {
      Thread.sleep(500);
      return "Task 2 completed";
    });

    Task<String> task3 = new Task<>(UUID.randomUUID(), group1, TaskType.READ, () -> {
      Thread.sleep(200);
      return "Task 3 completed";
    });

    Future<String> future1 = executor.submitTask(task1);
    Future<String> future2 = executor.submitTask(task2);
    Future<String> future3 = executor.submitTask(task3);

    System.out.println(future1.get());
    System.out.println(future2.get());
    System.out.println(future3.get());
  }
}

