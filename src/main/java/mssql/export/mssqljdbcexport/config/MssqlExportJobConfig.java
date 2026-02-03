package mssql.export.mssqljdbcexport.config;

import java.util.Base64;
import javax.sql.DataSource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mssql.export.mssqljdbcexport.dto.CategoryDto;
import mssql.export.mssqljdbcexport.dto.formDataDto;
import mssql.export.mssqljdbcexport.dto.formDto;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class MssqlExportJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;

    @Bean
    public Job exportJob(
            Step clipFormCategoryStep,
            Step formStep,
            Step formDataStep,
            JobExecutionListener jobCompletionNotificationListener
    ) {
        return new JobBuilder("exportJob", jobRepository)
                .listener(jobCompletionNotificationListener)
                .start(clipFormCategoryStep)
                .next(formStep)
                .next(formDataStep)
                .build();
    }

    @Bean
    public Step clipFormCategoryStep(
            JdbcCursorItemReader<CategoryDto> categoryReader
    ) {
        return new StepBuilder("categoryStep", jobRepository)
                .<CategoryDto, CategoryDto>chunk(500, transactionManager)
                .reader(categoryReader)
                .writer(categoryFileWriter())
                .build();
    }

    @Bean
    public Step formStep(
            JdbcCursorItemReader<formDto> clipFormReader
    ) {
        return new StepBuilder("exportStep", jobRepository)
                .<formDto, formDto>chunk(500, transactionManager)
                .reader(clipFormReader)
                .writer(formFileWriter())
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<CategoryDto> categoryReader(
            @Value("#{jobParameters['lastRunTime']}") String lastRunTime
    ) {
        return new JdbcCursorItemReaderBuilder<CategoryDto>()
                .name("categoryReader")
                .dataSource(dataSource)
                .sql("""
                        select sql
                        """)
                .queryArguments(lastRunTime != null ? lastRunTime : "1900-01-01 00:00:00")
                .rowMapper(new BeanPropertyRowMapper<>(CategoryDto.class))
                .build();
    }

    @Bean
    public FlatFileItemWriter<CategoryDto> categoryFileWriter() {
        return new FlatFileItemWriterBuilder<CategoryDto>()
                .name("categoryFileWriter")
                .resource(new FileSystemResource("C:/경로/category.csv"))
                .delimited().delimiter("|")
                .names("필드명")
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<formDto> formReader(
            @Value("#{jobParameters['lastRunTime']}") String lastRunTime
    ) {
        return new JdbcCursorItemReaderBuilder<formDto>()
                .name("formReader")
                .dataSource(dataSource)
                .sql("""
                        select sql
                        """)
                .queryArguments(lastRunTime != null ? lastRunTime : "1900-01-01 00:00:00")
                .rowMapper(new BeanPropertyRowMapper<>(formDto.class))
                .build();
    }

    @Bean
    public FlatFileItemWriter<formDto> formFileWriter() {
        return new FlatFileItemWriterBuilder<formDto>()
                .name("formFileWriter")
                .resource(new FileSystemResource("C:/경로/form.csv"))
                .delimited()
                .delimiter("|")
                .names("필드명")
                .build();
    }

    @Bean
    public Step formDataStep(
            JdbcCursorItemReader<formDataDto> formDataReader
    ) {
        return new StepBuilder("formDataStep", jobRepository)
                .<formDataDto, formDataDto>chunk(50,
                        transactionManager) // BLOB이 있으므로 청크 크기를 줄임 (50~100 권장)
                .reader(formDataReader)
                .processor(formDataProcessor())
                .writer(formDataFileWriter())
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<formDataDto> clipFormDataReader(
            @Value("#{jobParameters['lastRunTime']}") String lastRunTime
    ) {

        String sql = """
                select query
                """;

        return new JdbcCursorItemReaderBuilder<formDataDto>()
                .name("formDataReader")
                .dataSource(dataSource)
                .sql(sql)
                .queryArguments(lastRunTime != null ? lastRunTime : "1900-01-01 00:00:00")
                .rowMapper(new BeanPropertyRowMapper<>(formDataDto.class))
                .build();
    }

    @Bean
    public FlatFileItemWriter<formDataDto> formDataFileWriter() {
        return new FlatFileItemWriterBuilder<formDataDto>()
                .name("formDataFileWriter")
                .resource(new FileSystemResource("C:/경로/form_data.csv"))
                .delimited().delimiter("|")
                .names("필드명")
                .build();
    }

    @Bean
    public ItemProcessor<formDataDto, formDataDto> formDataProcessor() {
        return item -> {
            if (item.getFormData() != null && item.getFormData().length > 0) {
                item.setBase64FormData(Base64.getEncoder().encodeToString(item.getFormData()));

                item.setFormData(null);
            }

            return item;
        };
    }
}
