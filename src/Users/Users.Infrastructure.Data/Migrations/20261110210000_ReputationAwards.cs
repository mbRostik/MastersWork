using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Users.Infrastructure.Data.Migrations;

[DbContext(typeof(UserDbContext))]
[Migration("20261110210000_ReputationAwards")]
public class ReputationAwards : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable("ReputationAwards", table => new
        {
            PostId = table.Column<int>(type: "int", nullable: false)
        }, constraints: table => table.PrimaryKey("PK_ReputationAwards", row => row.PostId));
    }

    protected override void Down(MigrationBuilder migrationBuilder) => migrationBuilder.DropTable("ReputationAwards");
}
