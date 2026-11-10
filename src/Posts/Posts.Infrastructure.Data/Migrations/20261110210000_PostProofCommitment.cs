using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Posts.Infrastructure.Data.Migrations;

[DbContext(typeof(PostDbContext))]
[Migration("20261110210000_PostProofCommitment")]
public class PostProofCommitment : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // The old prototype used this column without an EF migration. Some
        // existing installations already have it, and their values must survive.
        migrationBuilder.Sql("""
            IF COL_LENGTH(N'dbo.Posts', N'Commitment') IS NULL
                ALTER TABLE [dbo].[Posts] ADD [Commitment] nvarchar(77) NULL;
            ELSE
                ALTER TABLE [dbo].[Posts] ALTER COLUMN [Commitment] nvarchar(77) NULL;
            """);

        migrationBuilder.AddColumn<byte[]>("CommitmentSalt", "Posts", "varbinary(32)", maxLength: 32, nullable: true);
        migrationBuilder.CreateIndex("IX_Posts_Commitment", "Posts", "Commitment", unique: true,
            filter: "[Commitment] IS NOT NULL AND [CommitmentSalt] IS NOT NULL");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropIndex("IX_Posts_Commitment", "Posts");
        migrationBuilder.DropColumn("CommitmentSalt", "Posts");
        migrationBuilder.Sql("ALTER TABLE [dbo].[Posts] ALTER COLUMN [Commitment] nvarchar(max) NULL;");
    }
}
