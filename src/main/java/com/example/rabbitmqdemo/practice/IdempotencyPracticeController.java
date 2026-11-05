package com.example.rabbitmqdemo.practice;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/practice/idempotency")
public class IdempotencyPracticeController {

    private final JdbcTemplate jdbcTemplate;

    public IdempotencyPracticeController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostMapping("/reset/{jobId}")
    public DemoJob reset(@PathVariable long jobId) {
        jdbcTemplate.update("DELETE FROM demo_job WHERE id = ?", jobId);

        jdbcTemplate.update("""
            INSERT INTO demo_job(id, status, execute_count)
            VALUES (?, 'PENDING', 0)
            """,
                jobId
        );

        return findJob(jobId);
    }

    @PostMapping("/ordinary")
    public UpdateResult ordinaryUpdate() {
        int affectedRows = jdbcTemplate.update("""
                    UPDATE demo_job
                    SET execute_count = execute_count + 1
                    WHERE id = 100
                """);

        return new UpdateResult(affectedRows, findJob(100));
    }

    @PostMapping("/conditional")
    public UpdateResult conditionalUpdate() {
        int affectedRows = jdbcTemplate.update("""
                UPDATE demo_job
                SET status = 'RUNNING',
                    execute_count = execute_count + 1
                WHERE id = 100
                   AND status = 'PENDING'
                """);

        return new UpdateResult(affectedRows, findJob(100));
    }

    private DemoJob findJob(long jobId) {
        return jdbcTemplate.queryForObject("""
                SELECT id, status, execute_count
                FROM demo_job
                WHERE id = ?
                """,
                (rs, rowNum) -> new DemoJob(
                        rs.getLong("id"),
                        rs.getString("status"),
                        rs.getInt("execute_count")
                ),
                jobId
        );
    }

    public record DemoJob(
            long id,
            String status,
            int executeCount
    ) {
    }

    public record UpdateResult(
            int affectedRows,
            DemoJob job
    ) {
    }

}
